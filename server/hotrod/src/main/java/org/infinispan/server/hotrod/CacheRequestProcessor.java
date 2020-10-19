package org.infinispan.server.hotrod;

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

import javax.security.auth.Subject;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.server.hotrod.HotRodServer.ExtendedCacheInfo;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;
import org.infinispan.server.hotrod.iteration.IterationState;
import org.infinispan.server.hotrod.logging.Log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

class CacheRequestProcessor extends BaseRequestProcessor {
   private static final Log log = LogFactory.getLog(CacheRequestProcessor.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();

   private final ClientListenerRegistry listenerRegistry;

   CacheRequestProcessor(Channel channel, Executor executor, HotRodServer server) {
      super(channel, executor, server);
      listenerRegistry = server.getClientListenerRegistry();
   }

   void ping(HotRodHeader header, Subject subject) {
      // we need to throw an exception when the cache is inaccessible
      // but ignore the default cache, because the client always pings the default cache first
      if (!header.cacheName.isEmpty()) {
         server.cache(server.getCacheInfo(header), header, subject);
      }
      writeResponse(header, header.encoder().pingResponse(header, server, channel, OperationStatus.Success));
   }

   void stats(HotRodHeader header, Subject subject) {
      AdvancedCache<byte[], byte[]> cache = server.cache(server.getCacheInfo(header), header, subject);
      executor.execute(() -> writeResponse(header, header.encoder().statsResponse(header, server, channel,
            cache.getStats(), server.getTransport(), SecurityActions.getCacheComponentRegistry(cache))));
   }

   void get(HotRodHeader header, Subject subject, byte[] key) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);

      getInternal(header, cache, key);
   }

   private void getInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key) {
      CompletableFuture<CacheEntry<byte[], byte[]>> get = cache.getCacheEntryAsync(key);
      if (get.isDone() && !get.isCompletedExceptionally()) {
         handleGet(header, get.join(), null);
      } else {
         get.whenComplete((result, throwable) -> handleGet(header, result, throwable));
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
         if (entry == null) {
            offset = 0;
         }
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
      metadata.version(cacheInfo.versionGenerator.generateNew());
      putInternal(header, cache, key, value, metadata.build());
   }

   private void putInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key, byte[] value, Metadata metadata) {
      cache.putAsync(key, value, metadata)
            .whenComplete((result, throwable) -> handlePut(header, result, throwable));
   }

   private void handlePut(HotRodHeader header, byte[] result, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else {
         writeSuccess(header, result);
      }
   }

   void replaceIfUnmodified(HotRodHeader header, Subject subject, byte[] key, long version, byte[] value, Metadata.Builder metadata) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      metadata.version(cacheInfo.versionGenerator.generateNew());
      replaceIfUnmodifiedInternal(header, cache, key, version, value, metadata.build());
   }

   private void replaceIfUnmodifiedInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key, long version, byte[] value, Metadata metadata) {
      cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getCacheEntryAsync(key)
            .whenComplete((entry, throwable) -> handleGetForReplaceIfUnmodified(header, cache, entry, version, value, metadata, throwable));
   }

   private void handleGetForReplaceIfUnmodified(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, CacheEntry<byte[], byte[]> entry, long version, byte[] value, Metadata metadata, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else if (entry != null) {
         byte[] prev = entry.getValue();
         NumericVersion streamVersion = new NumericVersion(version);
         if (entry.getMetadata().version().equals(streamVersion)) {
            cache.replaceAsync(entry.getKey(), prev, value, metadata)
                  .whenComplete((replaced, throwable2) -> {
                     if (throwable2 != null) {
                        writeException(header, throwable2);
                     } else if (replaced) {
                        writeSuccess(header, prev);
                     } else {
                        writeNotExecuted(header, prev);
                     }
                  });
         } else {
            writeNotExecuted(header, prev);
         }
      } else {
         writeNotExist(header);
      }
   }

   void replace(HotRodHeader header, Subject subject, byte[] key, byte[] value, Metadata.Builder metadata) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      metadata.version(cacheInfo.versionGenerator.generateNew());
      replaceInternal(header, cache, key, value, metadata.build());
   }

   private void replaceInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key, byte[] value, Metadata metadata) {
      // Avoid listener notification for a simple optimization
      // on whether a new version should be calculated or not.
      cache.withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getAsync(key)
            .whenComplete((prev, throwable) -> handleGetForReplace(header, cache, key, prev, value, metadata, throwable));
   }

   private void handleGetForReplace(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key, byte[] prev, byte[] value, Metadata metadata, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else if (prev != null) {
         // Generate new version only if key present
         cache.replaceAsync(key, value, metadata)
               .whenComplete((result, throwable1) -> handleReplace(header, result, throwable1));
      } else {
         writeNotExecuted(header);
      }
   }

   private void handleReplace(HotRodHeader header, byte[] result, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else if (result != null) {
         writeSuccess(header, result);
      } else {
         writeNotExecuted(header);
      }
   }

   void putIfAbsent(HotRodHeader header, Subject subject, byte[] key, byte[] value, Metadata.Builder metadata) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      metadata.version(cacheInfo.versionGenerator.generateNew());
      putIfAbsentInternal(header, cache, key, value, metadata.build());
   }

   private void putIfAbsentInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key, byte[] value, Metadata metadata) {
      cache.getAsync(key).whenComplete((prev, throwable) -> handleGetForPutIfAbsent(header, cache, key, prev, value, metadata, throwable));
   }

   private void handleGetForPutIfAbsent(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key, byte[] prev, byte[] value, Metadata metadata, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else if (prev == null) {
         // Generate new version only if key not present
         cache.putIfAbsentAsync(key, value, metadata)
               .whenComplete((result, throwable1) -> handlePutIfAbsent(header, result, throwable1));
      } else {
         writeNotExecuted(header, prev);
      }
   }

   private void handlePutIfAbsent(HotRodHeader header, byte[] result, Throwable throwable) {
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
      removeInternal(header, cache, key);
   }

   private void removeInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key) {
      cache.removeAsync(key).whenComplete((prev, throwable) -> handleRemove(header, prev, throwable));
   }

   private void handleRemove(HotRodHeader header, byte[] prev, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else if (prev != null) {
         writeSuccess(header, prev);
      } else {
         writeNotExist(header);
      }
   }

   void removeIfUnmodified(HotRodHeader header, Subject subject, byte[] key, long version) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      removeIfUnmodifiedInternal(header, cache, key, version);
   }

   private void removeIfUnmodifiedInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] key, long version) {
      cache.getCacheEntryAsync(key)
            .whenComplete((entry, throwable) -> handleGetForRemoveIfUnmodified(header, cache, entry, key, version, throwable));
   }

   private void handleGetForRemoveIfUnmodified(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, CacheEntry<byte[], byte[]> entry, byte[] key, long version, Throwable throwable) {
      if (throwable != null) {
         writeException(header, throwable);
      } else if (entry != null) {
         byte[] prev = entry.getValue();
         NumericVersion streamVersion = new NumericVersion(version);
         if (entry.getMetadata().version().equals(streamVersion)) {
            cache.removeAsync(key, prev).whenComplete((removed, throwable2) -> {
               if (throwable2 != null) {
                  writeException(header, throwable2);
               } else if (removed) {
                  writeSuccess(header, prev);
               } else {
                  writeNotExecuted(header, prev);
               }
            });
         } else {
            writeNotExecuted(header, prev);
         }
      } else {
         writeNotExist(header);
      }
   }

   void clear(HotRodHeader header, Subject subject) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      clearInternal(header, cache);
   }

   private void clearInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache) {
      cache.clearAsync().whenComplete((nil, throwable) -> {
         if (throwable != null) {
            writeException(header, throwable);
         } else {
            writeSuccess(header);
         }
      });
   }

   void putAll(HotRodHeader header, Subject subject, Map<byte[], byte[]> entries, Metadata.Builder metadata) {
      ExtendedCacheInfo cacheInfo = server.getCacheInfo(header);
      AdvancedCache<byte[], byte[]> cache = server.cache(cacheInfo, header, subject);
      putAllInternal(header, cache, entries, metadata.build());
   }

   private void putAllInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, Map<byte[], byte[]> entries, Metadata metadata) {
      cache.putAllAsync(entries, metadata).whenComplete((nil, throwable) -> handlePutAll(header, throwable));
   }

   private void handlePutAll(HotRodHeader header, Throwable throwable) {
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
      AdvancedCache<byte[], byte[]> cache = server.cache(server.getCacheInfo(header), header, subject);
      sizeInternal(header, cache);
   }

   private void sizeInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache) {
      cache.sizeAsync()
            .whenComplete((size, throwable) -> handleSize(header, size, throwable));
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
         if (trace) {
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
         if (trace) {
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

   void addClientListener(HotRodHeader header, Subject subject, byte[] listenerId, boolean includeCurrentState, String filterFactory, List<byte[]> filterParams, String converterFactory, List<byte[]> converterParams, boolean useRawData, int listenerInterests) {
      AdvancedCache<byte[], byte[]> cache = server.cache(server.getCacheInfo(header), header, subject);
      CompletionStage<Void> stage = listenerRegistry.addClientListener(channel, header, listenerId, cache,
            includeCurrentState, filterFactory, filterParams, converterFactory, converterParams, useRawData,
            listenerInterests);
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
      });
   }

   void removeClientListener(HotRodHeader header, Subject subject, byte[] listenerId) {
      AdvancedCache<byte[], byte[]> cache = server.cache(server.getCacheInfo(header), header, subject);
      removeClientListenerInternal(header, cache, listenerId);
   }

   private void removeClientListenerInternal(HotRodHeader header, AdvancedCache<byte[], byte[]> cache, byte[] listenerId) {
      server.getClientListenerRegistry().removeClientListener(listenerId, cache)
            .whenComplete((success, t) -> {
               if (t != null) {
                  writeException(header, t);
               } else {
                  if (success == Boolean.TRUE) {
                     writeSuccess(header);
                  } else {
                     writeNotExecuted(header);
                  }
               }
            });
   }

   void iterationStart(HotRodHeader header, Subject subject, byte[] segmentMask, String filterConverterFactory,
                       List<byte[]> filterConverterParams, int batch, boolean includeMetadata) {
      AdvancedCache<byte[], byte[]> cache = server.cache(server.getCacheInfo(header), header, subject);
      executor.execute(() -> {
         try {
            IterationState iterationState = server.getIterationManager().start(cache, segmentMask != null ? BitSet.valueOf(segmentMask) : null,
                  filterConverterFactory, filterConverterParams, header.getValueMediaType(), batch, includeMetadata);
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
            IterableIterationResult iterationResult = server.getIterationManager().next(iterationId);
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

   EmbeddedCacheManager getCacheManager() {
      return server.getCacheManager();
   }
}
