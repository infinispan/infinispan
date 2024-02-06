package org.infinispan.server.hotrod;

import javax.security.auth.Subject;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.BloomFilter;
import org.infinispan.commons.util.IntSets;
import org.infinispan.commons.util.MurmurHash3BloomFilter;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.metadata.Metadata;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.transport.ConnectionMetadata;
import org.infinispan.server.hotrod.HotRodServer.ExtendedCacheInfo;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.iteration.IterableIterationResult;
import org.infinispan.server.iteration.IterationState;
import org.infinispan.stats.ClusterCacheStats;
import org.infinispan.stats.Stats;
import org.infinispan.telemetry.InfinispanSpan;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.telemetry.InfinispanTelemetry;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

class CacheRequestProcessor extends BaseRequestProcessor {
   private static final Log log = LogFactory.getLog(CacheRequestProcessor.class, Log.class);

   private final ClientListenerRegistry listenerRegistry;
   private final InfinispanTelemetry telemetryService;

   private final ConcurrentMap<String, BloomFilter<byte[]>> bloomFilters = new ConcurrentHashMap<>();

   CacheRequestProcessor(Channel channel, Executor executor, HotRodServer server, InfinispanTelemetry telemetryService) {
      super(channel, executor, server);
      this.listenerRegistry = server.getClientListenerRegistry();
      this.telemetryService = telemetryService;
   }

   void ping(HotRodHeader header, Subject subject) {
      // we need to throw an exception when the cache is inaccessible
      // but ignore the default cache, because the client always pings the default cache first
      if (!header.cacheName.isEmpty()) {
         server.cache(server.getCacheInfo(header), header, subject);
      }
      ConnectionMetadata metadata = ConnectionMetadata.getInstance(channel);
      metadata.protocolVersion(HotRodVersion.forVersion(header.version).toString());
      writeResponse(header, header.encoder().pingResponse(header, server, channel, OperationStatus.Success));
   }

   void stats(HotRodHeader header, Subject subject) {
      AdvancedCache<byte[], byte[]> cache = server.cache(server.getCacheInfo(header), header, subject);
      executor.execute(() -> blockingStats(header, cache));
   }

   private void blockingStats(HotRodHeader header, AdvancedCache<byte[], byte[]> cache) {
      try {
         Stats stats = cache.getStats();
         ClusterCacheStats clusterCacheStats =
               SecurityActions.getCacheComponentRegistry(cache).getComponent(ClusterCacheStats.class);
         ByteBuf buf = header.encoder().statsResponse(header, server, channel, stats, server.getTransport(),
               clusterCacheStats);
         writeResponse(header, buf);
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   void get(HotRodHeader header, Subject subject, byte[] key) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);

      getInternal(header, cache, key);
   }

   void updateBloomFilter(HotRodHeader header, Subject subject, byte[] bloomArray) {
      try {
         BloomFilter<byte[]> filter = bloomFilters.get(header.cacheName);
         if (filter != null) {
            if (log.isTraceEnabled()) {
               log.tracef("Updating bloom filter %s found for cache %s", filter, header.cacheName);
            }
            filter.setBits(IntSets.from(bloomArray));
            if (log.isTraceEnabled()) {
               log.tracef("Updated bloom filter %s for cache %s", filter, header.cacheName);
            }
            writeSuccess(header);
         } else {
            if (log.isTraceEnabled()) {
               log.tracef("There was no bloom filter for cache %s from client", header.cacheName);
            }
            writeNotExecuted(header);
         }
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   private void getInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key) {
      CompletableFuture<CacheEntry<byte[], byte[]>> get = cache.getCacheEntryAsync(key);
      if (get.isDone() && !get.isCompletedExceptionally()) {
         handleGet(header, get.join(), null);
      } else {
         get.whenComplete((result, throwable) -> handleGet(header, result, throwable));
      }
   }

   void addToFilter(String cacheName, byte[] key) {
      BloomFilter<byte[]> bloomFilter = bloomFilters.get(cacheName);
      // TODO: Need to think harder about this because we could have a concurrent write as we are doing our get
      // and we could have just have had an invalidation come through that didn't pass the bloom filter
      // I believe this has to go at the beginning of the get command before we get a value or exception
      // We can fix this by adding a temporary check that if a get is being performed and the listener checks the
      // bloom filter for the key to return a bit saying to not cache the returned value
      if (bloomFilter != null) {
         bloomFilter.addToFilter(key);
      }
   }

   private void handleGet(HotRodHeader header, CacheEntry<byte[], byte[]> result, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else {
         if (result == null) {
            writeNotExist(header);
         } else {
            try {
               switch (header.op) {
                  case GET:
                     writeResponse(header, header.encoder().valueResponse(header, server, channel, OperationStatus.Success, result.getValue()));
                     break;
                  case GET_WITH_VERSION:
                     NumericVersion numericVersion = (NumericVersion) result.getMetadata().version();
                     long version;
                     if (numericVersion != null) {
                        version = numericVersion.getVersion();
                     } else {
                        version = 0;
                     }
                     writeResponse(header, header.encoder().valueWithVersionResponse(header, server, channel, result.getValue(), version));
                     break;
                  default:
                     throw new IllegalStateException();
               }
            } catch (Throwable t2) {
               writeException(header, t2);
            }
         }
      }
   }

   void getWithMetadata(HotRodHeader header, Subject subject, byte[] key, int offset) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      getWithMetadataInternal(header, cache, key, offset);
   }

   private void getWithMetadataInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key, int offset) {
      // We have to set before retrieving the value in case of a concurrent write
      // This can cause unneeded invalidations for a miss, but is required for consistency
      addToFilter(header.cacheName, key);
      CompletableFuture<CacheEntry<byte[], byte[]>> get = cache.getCacheEntryAsync(key);
      if (get.isDone() && !get.isCompletedExceptionally()) {
         handleGetWithMetadata(header, offset, get.join(), null);
      } else {
         get.whenComplete((ce, throwable) -> handleGetWithMetadata(header, offset, ce, throwable));
      }
   }

   private void handleGetWithMetadata(HotRodHeader header, int offset, CacheEntry<byte[], byte[]> entry, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
         return;
      }
      if (entry == null) {
         writeNotExist(header);
      } else if (header.op == HotRodOperation.GET_WITH_METADATA) {
         assert offset == 0;
         writeResponse(header, header.encoder().getWithMetadataResponse(header, server, channel, entry));
      } else {
         writeResponse(header, header.encoder().getStreamResponse(header, server, channel, offset, entry));
      }
   }

   void containsKey(HotRodHeader header, Subject subject, byte[] key) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      containsKeyInternal(header, cache, key);
   }

   private void containsKeyInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key) {
      CompletableFuture<Boolean> contains = cache.containsKeyAsync(key);
      if (contains.isDone() && !contains.isCompletedExceptionally()) {
         handleContainsKey(header, contains.join(), null);
      } else {
         contains.whenComplete((result, throwable) -> handleContainsKey(header, result, throwable));
      }
   }

   private void handleContainsKey(HotRodHeader header, Boolean result, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else if (result) {
         writeSuccess(header);
      } else {
         writeNotExist(header);
      }
   }

   void put(HotRodHeader header, Subject subject, byte[] key, byte[] value, Metadata.Builder metadata) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      InfinispanSpan<CacheEntry<?, ?>> span = requestStart(header, cacheInfo.getInfinispanSpanAttributes());
      try (var ignored = span.makeCurrent()) {
         metadata.version(cacheInfo.versionGenerator.generateNew());
         putInternal(header, cache, key, value, metadata.build(), span);
      }
   }

   private void putInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key, byte[] value,
                            Metadata metadata, InfinispanSpan<CacheEntry<?, ?>> span) {
      cache.putAsyncEntry(key, value, metadata)
            .whenComplete((ce, throwable) -> handlePut(header, ce, throwable))
            .whenComplete(span);
   }

   private void handlePut(HotRodHeader header, CacheEntry<byte[], byte[]> ce, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else {
         writeSuccess(header, ce);
      }
   }

   void replaceIfUnmodified(HotRodHeader header, Subject subject, byte[] key, long version, byte[] value, Metadata.Builder metadata) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      InfinispanSpan<ConditionalResponse> span = requestStart(header, cacheInfo.getInfinispanSpanAttributes());
      try (var ignored = span.makeCurrent()) {
         metadata.version(cacheInfo.versionGenerator.generateNew());
         replaceIfUnmodifiedInternal(header, cache, key, version, value, metadata.build(), span);
      }
   }

   private void replaceIfUnmodifiedInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key,
                                            long version, byte[] value, Metadata metadata, InfinispanSpan<ConditionalResponse> span) {
      cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getCacheEntryAsync(key)
            .thenCompose(entry -> replaceIfUnmodifiedAfterGet(cache, entry, version, value, metadata))
            .whenComplete((response, throwable) -> handleConditionalResponse(header, response, throwable))
            .whenComplete(span);
   }

   private CompletionStage<ConditionalResponse> replaceIfUnmodifiedAfterGet(AdvancedCache<byte[], byte[]> cache,
                                                                            CacheEntry<byte[], byte[]> entry,
                                                                            long version, byte[] value,
                                                                            Metadata metadata) {
      if (entry == null) {
         //entry does not exist, replace fails
         return CompletableFuture.completedFuture(new ConditionalResponse(false, null));
      }
      NumericVersion streamVersion = new NumericVersion(version);
      if (!streamVersion.equals(entry.getMetadata().version())) {
         //version does not match, replace fails
         return CompletableFuture.completedFuture(new ConditionalResponse(false, entry));
      }
      return cache.replaceAsync(entry.getKey(), entry.getValue(), value, metadata)
            .thenApply(replaced -> new ConditionalResponse(replaced, entry));
   }

   private void handleConditionalResponse(HotRodHeader header, ConditionalResponse response, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else if (response.result) {
         assert response.entry != null;
         writeSuccess(header, response.entry);
      } else {
         if (response.entry == null) {
            writeNotExist(header);
         } else {
            writeNotExecuted(header, response.entry);
         }
      }
   }

   void replace(HotRodHeader header, Subject subject, byte[] key, byte[] value, Metadata.Builder metadata) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      InfinispanSpan<CacheEntry<?, ?>> span = requestStart(header, cacheInfo.getInfinispanSpanAttributes());
      try (var ignored = span.makeCurrent()) {
         metadata.version(cacheInfo.versionGenerator.generateNew());
         replaceInternal(header, cache, key, value, metadata.build(), span);
      }
   }

   private void replaceInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key, byte[] value,
                                Metadata metadata, InfinispanSpan<CacheEntry<?, ?>> span) {
      // Avoid listener notification for a simple optimization
      // on whether a new version should be calculated or not.
      cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getAsync(key)
            .thenCompose(prev -> replaceAfterGet(cache, prev != null, key, value, metadata))
            .whenComplete((cacheEntry, throwable) -> handleReplaceIfExists(header, cacheEntry, throwable))
            .whenComplete(span);
   }

   private CompletionStage<CacheEntry<byte[], byte[]>> replaceAfterGet(AdvancedCache<byte[], byte[]> cache, boolean exists, byte[] key, byte[] value, Metadata metadata) {
      return exists ?
            cache.replaceAsyncEntry(key, value, metadata) :
            CompletableFutures.completedNull();
   }

   private void handleReplaceIfExists(HotRodHeader header, CacheEntry<byte[], byte[]> cacheEntry, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else if (cacheEntry == null) {
         writeNotExecuted(header);
      } else {
         writeSuccess(header, cacheEntry);
      }
   }

   void putIfAbsent(HotRodHeader header, Subject subject, byte[] key, byte[] value, Metadata.Builder metadata) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      InfinispanSpan<CacheEntry<?, ?>> span = requestStart(header, cacheInfo.getInfinispanSpanAttributes());
      try (var ignored = span.makeCurrent()) {
         metadata.version(cacheInfo.versionGenerator.generateNew());
         putIfAbsentInternal(header, cache, key, value, metadata.build(), span);
      }
   }

   private void putIfAbsentInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key, byte[] value,
                                    Metadata metadata, InfinispanSpan<CacheEntry<?, ?>> span) {
      cache.putIfAbsentAsyncEntry(key, value, metadata)
            .whenComplete((prev, throwable) -> handlePutIfAbsent(header, prev, throwable))
            .whenComplete(span);
   }

   private void handlePutIfAbsent(HotRodHeader header, CacheEntry<byte[], byte[]> result, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else if (result == null) {
         writeSuccess(header);
      } else {
         writeNotExecuted(header, result);
      }
   }

   void remove(HotRodHeader header, Subject subject, byte[] key) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      InfinispanSpan<CacheEntry<?, ?>> span = requestStart(header, cacheInfo.getInfinispanSpanAttributes());
      try (var ignored = span.makeCurrent()) {
         removeInternal(header, cache, key, span);
      }
   }

   private void removeInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key,
                               InfinispanSpan<CacheEntry<?, ?>> span) {
      cache.removeAsyncEntry(key)
            .whenComplete((ce, throwable) -> handleRemove(header, ce, throwable))
            .whenComplete(span);
   }

   private void handleRemove(HotRodHeader header, CacheEntry<byte[], byte[]> ce, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else if (ce != null) {
         writeSuccess(header, ce);
      } else {
         writeNotExist(header);
      }
   }

   void removeIfUnmodified(HotRodHeader header, Subject subject, byte[] key, long version) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      InfinispanSpan<ConditionalResponse> span = requestStart(header, cacheInfo.getInfinispanSpanAttributes());
      try (var ignored = span.makeCurrent()) {
         removeIfUnmodifiedInternal(header, cache, key, version, span);
      }
   }

   private void removeIfUnmodifiedInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key,
                                           long version, InfinispanSpan<ConditionalResponse> span) {
      cache.getCacheEntryAsync(key)
            .thenCompose(cacheEntry -> removeIfUnmodifiedAfterGet(cache, cacheEntry, version))
            .whenComplete((response, throwable) -> handleConditionalResponse(header, response, throwable))
            .whenComplete(span);
   }

   private CompletionStage<ConditionalResponse> removeIfUnmodifiedAfterGet(AdvancedCache<byte[], byte[]> cache,
                                                                           CacheEntry<byte[], byte[]> entry,
                                                                           long version) {
      if (entry == null) {
         return CompletableFuture.completedFuture(new ConditionalResponse(false, null));
      }
      NumericVersion streamVersion = new NumericVersion(version);
      if (!streamVersion.equals(entry.getMetadata().version())) {
         //version does not match, replace fails
         return CompletableFuture.completedFuture(new ConditionalResponse(false, entry));
      }
      return cache.removeAsync(entry.getKey(), entry.getValue())
            .thenApply(removed -> new ConditionalResponse(removed, entry));
   }

   void clear(HotRodHeader header, Subject subject) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      InfinispanSpan<Void> span = requestStart(header, cacheInfo.getInfinispanSpanAttributes());
      try (var ignored = span.makeCurrent()) {
         clearInternal(header, cache, span);
      }
   }

   private void clearInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, InfinispanSpan<Void> span) {
      cache.clearAsync()
            .whenComplete((unused, throwable) -> handleGenericResponse(header, throwable))
            .whenComplete(span);
   }

   void putAll(HotRodHeader header, Subject subject, Map<byte[], byte[]> entries, Metadata.Builder metadata) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      InfinispanSpan<Void> span = requestStart(header, cacheInfo.getInfinispanSpanAttributes());
      try (var ignored = span.makeCurrent()) {
         metadata.version(cacheInfo.versionGenerator.generateNew());
         putAllInternal(header, cache, entries, metadata.build(), span);
      }
   }

   private void putAllInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, Map<byte[], byte[]> entries,
                               Metadata metadata, InfinispanSpan<Void> span) {
      cache.putAllAsync(entries, metadata)
            .whenComplete((nil, throwable) -> handleGenericResponse(header, throwable))
            .whenComplete(span);
   }

   private void handleGenericResponse(HotRodHeader header, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else {
         writeSuccess(header);
      }
   }

   void getAll(HotRodHeader header, Subject subject, Set<?> keys) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      getAllInternal(header, cache, keys);
   }

   private void getAllInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, Set<?> keys) {
      cache.getAllAsync(keys)
            .whenComplete((map, throwable) -> handleGetAll(header, map, throwable));
   }

   private void handleGetAll(HotRodHeader header, Map<byte[], byte[]> map, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else {
         writeResponse(header, header.encoder().getAllResponse(header, server, channel, map));
      }
   }

   void size(HotRodHeader header, Subject subject) {
      var cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      InfinispanSpan<Long> span = requestStart(header, cacheInfo.getInfinispanSpanAttributes());
      try (var ignored = span.makeCurrent()) {
         sizeInternal(header, cache, span);
      }
   }

   private void sizeInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, InfinispanSpan<Long> span) {
      cache.sizeAsync()
            .whenComplete((size, throwable) -> handleSize(header, size, throwable))
            .whenComplete(span);
   }

   private void handleSize(HotRodHeader header, Long size, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else {
         writeResponse(header, header.encoder().unsignedLongResponse(header, server, channel, size));
      }
   }

   void bulkGet(HotRodHeader header, Subject subject, int size) {
      AdvancedCache<byte[], byte[]> cache = server.cache(server.getCacheInfo(header), header, subject);
      executor.execute(() -> bulkGetInternal(header, cache, size));
   }

   private void bulkGetInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, int size) {
      try {
         if (log.isTraceEnabled()) {
            log.tracef("About to create bulk response count = %d", size);
         }
         writeResponse(header, header.encoder().bulkGetResponse(header, server, channel, size, cache.entrySet()));
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   void bulkGetKeys(HotRodHeader header, Subject subject, int scope) {
      AdvancedCache<byte[], byte[]> cache = server.cache(server.getCacheInfo(header), header, subject);
      executor.execute(() -> bulkGetKeysInternal(header, cache, scope));
   }

   private void bulkGetKeysInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, int scope) {
      try {
         if (log.isTraceEnabled()) {
            log.tracef("About to create bulk get keys response scope = %d", scope);
         }
         writeResponse(header, header.encoder().bulkGetKeysResponse(header, server, channel, cache.keySet().iterator()));
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   void query(HotRodHeader header, Subject subject, byte[] queryBytes) {
      AdvancedCache<byte[], byte[]> cache = server.cache(server.getCacheInfo(header), header, subject);
      executor.execute(() -> queryInternal(header, cache, queryBytes));
   }

   private void queryInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] queryBytes) {
      try {
         byte[] queryResult = server.query(cache, queryBytes);
         writeResponse(header, header.encoder().valueResponse(header, server, channel, OperationStatus.Success, queryResult));
      } catch (Throwable t) {
         writeException(header, t);
      }
   }

   void addClientListener(HotRodHeader header, Subject subject, byte[] listenerId, boolean includeCurrentState,
                          String filterFactory, List<byte[]> filterParams, String converterFactory,
                          List<byte[]> converterParams, boolean useRawData, int listenerInterests, int bloomBits) {
      var cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      var span = requestStart(header, cacheInfo.getInfinispanSpanAttributes());
      try (var ignored = span.makeCurrent()) {
         BloomFilter<byte[]> bloomFilter = null;
         if (bloomBits > 0) {
            bloomFilter = MurmurHash3BloomFilter.createConcurrentFilter(bloomBits);
            BloomFilter<byte[]> priorFilter = bloomFilters.putIfAbsent(header.cacheName, bloomFilter);
            assert priorFilter == null;
         }
         CompletionStage<Void> stage = listenerRegistry.addClientListener(channel, header, listenerId, cache,
               includeCurrentState, filterFactory, filterParams, converterFactory, converterParams, useRawData,
               listenerInterests, bloomFilter);
         stage.whenComplete((ignore, cause) -> {
            if (cause != null) {
               log.trace("Failed to add listener", cause);
               if (cause instanceof CompletionException) {
                  writeException(header, cause.getCause());
               } else {
                  writeException(header, cause);
               }
            } else {
               writeSuccess(header);
            }
         }).whenComplete(span);
      }
   }

   void removeClientListener(HotRodHeader header, Subject subject, byte[] listenerId) {
      var cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      InfinispanSpan<Boolean> span = requestStart(header, cacheInfo.getInfinispanSpanAttributes());
      try (var ignored = span.makeCurrent()) {
         removeClientListenerInternal(header, cache, listenerId, span);
      }
   }

   private void removeClientListenerInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache,
                                             byte[] listenerId, InfinispanSpan<Boolean> span) {
      server.getClientListenerRegistry().removeClientListener(listenerId, cache)
            .whenComplete((success, throwable) -> {
               if (throwable != null) {
                  writeException(header, throwable);
               } else {
                  if (success == Boolean.TRUE) {
                     writeSuccess(header);
                  } else {
                     writeNotExecuted(header);
                  }
               }
            })
            .whenComplete(span);
   }

   void iterationStart(HotRodHeader header, Subject subject, byte[] segmentMask, String filterConverterFactory,
                       List<byte[]> filterConverterParams, int batch, boolean includeMetadata) {
      AdvancedCache<byte[], byte[]> cache = server.cache(server.getCacheInfo(header), header, subject);
      executor.execute(() -> {
         try {
            IterationState iterationState = server.getIterationManager().start(cache, segmentMask != null ? BitSet.valueOf(segmentMask) : null,
                  filterConverterFactory, filterConverterParams, header.getValueMediaType(), batch, includeMetadata, DeliveryGuarantee.EXACTLY_ONCE, null);
            iterationState.getReaper().registerChannel(channel);
            writeResponse(header, header.encoder().iterationStartResponse(header, server, channel, iterationState.getId()));
         } catch (Throwable t) {
            writeException(header, t);
         }
      });
   }

   void iterationNext(HotRodHeader header, Subject subject, String iterationId) {
      executor.execute(() -> {
         try {
            IterableIterationResult iterationResult = server.getIterationManager().next(iterationId, -1);
            writeResponse(header, header.encoder().iterationNextResponse(header, server, channel, iterationResult));
         } catch (Throwable t) {
            writeException(header, t);
         }
      });
   }

   void iterationEnd(HotRodHeader header, Subject subject, String iterationId) {
      executor.execute(() -> {
         try {
            IterationState removed = server.getIterationManager().close(iterationId);
            writeResponse(header, header.encoder().emptyResponse(header, server, channel, removed != null ? OperationStatus.Success : OperationStatus.InvalidIteration));
         } catch (Throwable t) {
            writeException(header, t);
         }
      });
   }

   void putStream(HotRodHeader header, Subject subject, byte[] key, ByteBuf buf, long version, Metadata.Builder metadata) {
      try {
         byte[] value = new byte[buf.readableBytes()];
         buf.readBytes(value);
         if (version == 0) { // Normal put
            put(header, subject, key, value, metadata);
         } else if (version < 0) { // putIfAbsent
            putIfAbsent(header, subject, key, value, metadata);
         } else { // versioned replace
            replaceIfUnmodified(header, subject, key, version, value, metadata);
         }
      } finally {
         buf.release();
      }
   }

   private <T> InfinispanSpan<T> requestStart(HotRodHeader header, InfinispanSpanAttributes spanAttributes) {
      return telemetryService.startTraceRequest(header.op.name(), spanAttributes, header);
   }

   private static class ConditionalResponse {
      final boolean result;
      final CacheEntry<byte[], byte[]> entry;

      ConditionalResponse(boolean result, CacheEntry<byte[], byte[]> entry) {
         this.result = result;
         this.entry = entry;
      }
   }
}
