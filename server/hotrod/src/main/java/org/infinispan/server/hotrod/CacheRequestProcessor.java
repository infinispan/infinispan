package org.infinispan.server.hotrod;

import java.util.BitSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.context.Flag;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.infinispan.server.hotrod.HotRodServer.CacheInfo;
import org.infinispan.server.hotrod.iteration.IterableIterationResult;
import org.infinispan.server.hotrod.logging.Log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

@Listener
class CacheRequestProcessor extends BaseRequestProcessor {
   private static final Log log = LogFactory.getLog(CacheRequestProcessor.class, Log.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final Flag[] SKIP_STATISTICS = new Flag[]{Flag.SKIP_STATISTICS};

   private final HotRodServer server;
   private final ClientListenerRegistry listenerRegistry;

   CacheRequestProcessor(Channel channel, Executor executor, HotRodServer server) {
      super(channel, executor);
      this.server = server;
      listenerRegistry = server.getClientListenerRegistry();
      SecurityActions.addListener(server.getCacheManager(), this);
   }

   @CacheStopped
   public void cacheStopped(CacheStoppedEvent event) {
      server.cacheStopped(event.getCacheName());
   }

   private boolean isBlockingRead(CacheDecodeContext cdc, CacheInfo info) {
      return info.persistence && !cdc.decoder.isSkipCacheLoad(cdc.header);
   }

   private boolean isBlockingWrite(CacheDecodeContext cdc) {
      CacheInfo info = server.getCacheInfo(cdc);
      // Note: cache store cannot be skipped (yet)
      return info.persistence || info.indexing && !cdc.decoder.isSkipIndexing(cdc.header);
   }

   void get(CacheDecodeContext cdc) {
      // This request is very fast, try to satisfy immediately
      CacheInfo info = server.getCacheInfo(cdc);
      CacheEntry<byte[], byte[]> entry = info.localNonBlocking(cdc.subject).getCacheEntry(cdc.key);
      if (entry != null) {
         handleGet(cdc, entry, null);
      } else if (isBlockingRead(cdc, info)) {
         executor.execute(() -> getInternal(cdc));
      } else {
         getInternal(cdc);
      }
   }

   private void getInternal(CacheDecodeContext cdc) {
      cdc.cache().withFlags(SKIP_STATISTICS).getCacheEntryAsync(cdc.key)
            .whenComplete((result, throwable) -> handleGet(cdc, result, throwable));
   }

   private void handleGet(CacheDecodeContext cdc, CacheEntry<byte[], byte[]> result, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else {
         try {
            writeResponse(cdc.decoder.createGetResponse(cdc.header, result));
         } catch (Throwable t2) {
            writeException(cdc, t2);
         }
      }
   }

   void getKeyMetadata(CacheDecodeContext cdc) {
      // This request is very fast, try to satisfy immediately
      CacheInfo info = server.getCacheInfo(cdc);
      CacheEntry<byte[], byte[]> entry = info.localNonBlocking(cdc.subject).getCacheEntry(cdc.key);
      if (entry != null) {
         handleGetKeyMetadata(cdc, entry, null);
      } else if (isBlockingRead(cdc, info)) {
         executor.execute(() -> getKeyMetadataInternal(cdc));
      } else {
         getKeyMetadataInternal(cdc);
      }
   }

   private void getKeyMetadataInternal(CacheDecodeContext cdc) {
      cdc.cache().withFlags(SKIP_STATISTICS).getCacheEntryAsync(cdc.key)
            .whenComplete((ce, throwable) -> handleGetKeyMetadata(cdc, ce, throwable));
   }

   private void handleGetKeyMetadata(CacheDecodeContext cdc, CacheEntry<byte[], byte[]> ce, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
         return;
      }
      OperationStatus status = ce == null ? OperationStatus.KeyDoesNotExist : OperationStatus.Success;
      if (cdc.header.op == HotRodOperation.GET_WITH_METADATA) {
         writeResponse(new GetWithMetadataResponse(cdc.header.version, cdc.header.messageId, cdc.header.cacheName, cdc.header.clientIntel,
               cdc.header.op, status, cdc.header.topologyId, ce));
      } else {
         int offset = ce == null ? 0 : (Integer) cdc.operationDecodeContext;
         writeResponse(new GetStreamResponse(cdc.header.version, cdc.header.messageId, cdc.header.cacheName, cdc.header.clientIntel,
               cdc.header.op, status, cdc.header.topologyId, offset, ce));
      }
   }

   void containsKey(CacheDecodeContext cdc) {
      // This request is very fast, try to satisfy immediately
      CacheInfo info = server.getCacheInfo(cdc);
      boolean contains = info.localNonBlocking(cdc.subject).containsKey(cdc.key);
      if (contains) {
         writeSuccess(cdc, null);
      } else if (isBlockingRead(cdc, info)) {
         executor.execute(() -> containsKeyInternal(cdc));
      } else {
         containsKeyInternal(cdc);
      }
   }

   private void containsKeyInternal(CacheDecodeContext cdc) {
      cdc.cache().withFlags(SKIP_STATISTICS).containsKeyAsync(cdc.key)
            .whenComplete((result, throwable) -> handleContainsKey(cdc, result, throwable));
   }

   private void handleContainsKey(CacheDecodeContext cdc, Boolean result, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else if (result) {
         writeSuccess(cdc, null);
      } else {
         writeNotExist(cdc);
      }
   }

   void put(CacheDecodeContext cdc) {
      if (isBlockingWrite(cdc)) {
         executor.execute(() -> putInternal(cdc));
      } else {
         putInternal(cdc);
      }
   }

   private void putInternal(CacheDecodeContext cdc) {
      cdc.cache().putAsync(cdc.key, (byte[]) cdc.operationDecodeContext, cdc.buildMetadata())
            .whenComplete((result, throwable) -> handlePut(cdc, result, throwable));
   }

   private void handlePut(CacheDecodeContext cdc, byte[] result, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else {
         writeSuccess(cdc, result);
      }
   }

   void replaceIfUnmodified(CacheDecodeContext cdc) {
      if (isBlockingWrite(cdc)) {
         executor.execute(() -> replaceIfUnmodifiedInternal(cdc));
      } else {
         replaceIfUnmodifiedInternal(cdc);
      }
   }

   private void replaceIfUnmodifiedInternal(CacheDecodeContext cdc) {
      cdc.cache().withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getCacheEntryAsync(cdc.key)
            .whenComplete((entry, throwable) -> handleGetForReplaceIfUnmodified(cdc, entry, throwable));
   }

   private void handleGetForReplaceIfUnmodified(CacheDecodeContext cdc, CacheEntry<byte[], byte[]> entry, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else if (entry != null) {
         byte[] prev = entry.getValue();
         NumericVersion streamVersion = new NumericVersion(cdc.params.streamVersion);
         if (entry.getMetadata().version().equals(streamVersion)) {
            cdc.cache().replaceAsync(cdc.key, prev, (byte[]) cdc.operationDecodeContext, cdc.buildMetadata())
                  .whenComplete((replaced, throwable2) -> {
                     if (throwable2 != null) {
                        writeException(cdc, throwable2);
                     } else if (replaced) {
                        writeSuccess(cdc, prev);
                     } else {
                        writeNotExecuted(cdc, prev);
                     }
                  });
         } else {
            writeNotExecuted(cdc, prev);
         }
      } else {
         writeNotExist(cdc);
      }
   }

   void replace(CacheDecodeContext cdc) {
      if (isBlockingWrite(cdc)) {
         executor.execute(() -> replaceInternal(cdc));
      } else {
         replaceInternal(cdc);
      }
   }

   private void replaceInternal(CacheDecodeContext cdc) {
      // Avoid listener notification for a simple optimization
      // on whether a new version should be calculated or not.
      cdc.cache().withFlags(Flag.SKIP_LISTENER_NOTIFICATION).getAsync(cdc.key)
            .whenComplete((prev, throwable) -> handleGetForReplace(cdc, prev, throwable));
   }

   private void handleGetForReplace(CacheDecodeContext cdc, byte[] prev, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else if (prev != null) {
         // Generate new version only if key present
         cdc.cache().replaceAsync(cdc.key, (byte[]) cdc.operationDecodeContext, cdc.buildMetadata())
               .whenComplete((result, throwable1) -> handleReplace(cdc, result, throwable1));
      } else {
         writeNotExecuted(cdc, null);
      }
   }

   private void handleReplace(CacheDecodeContext cdc, byte[] result, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else if (result != null) {
         writeSuccess(cdc, result);
      } else {
         writeNotExecuted(cdc, null);
      }
   }

   void putIfAbsent(CacheDecodeContext cdc) {
      if (isBlockingWrite(cdc)) {
         executor.execute(() -> putIfAbsent(cdc));
      } else {
         putIfAbsentInternal(cdc);
      }
   }

   private void putIfAbsentInternal(CacheDecodeContext cdc) {
      cdc.cache().getAsync(cdc.key).whenComplete((prev, throwable) -> handleGetForPutIfAbsent(cdc, prev, throwable));
   }

   private void handleGetForPutIfAbsent(CacheDecodeContext cdc, byte[] prev, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else if (prev == null) {
         // Generate new version only if key not present
         cdc.cache().putIfAbsentAsync(cdc.key, (byte[]) cdc.operationDecodeContext, cdc.buildMetadata())
               .whenComplete((result, throwable1) -> handlePutIfAbsent(cdc, result, throwable1));
      } else {
         writeNotExecuted(cdc, prev);
      }
   }

   private void handlePutIfAbsent(CacheDecodeContext cdc, byte[] result, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else if (result == null) {
         writeSuccess(cdc, null);
      } else {
         writeNotExecuted(cdc, result);
      }
   }

   void remove(CacheDecodeContext cdc) {
      if (isBlockingWrite(cdc)) {
         executor.execute(() -> removeInternal(cdc));
      } else {
         removeInternal(cdc);
      }
   }

   private void removeInternal(CacheDecodeContext cdc) {
      cdc.cache().removeAsync(cdc.key).whenComplete((prev, throwable) -> handleRemove(cdc, prev, throwable));
   }

   private void handleRemove(CacheDecodeContext cdc, byte[] prev, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else if (prev != null) {
         writeSuccess(cdc, prev);
      } else {
         writeNotExist(cdc);
      }
   }

   void removeIfUnmodified(CacheDecodeContext cdc) {
      if (isBlockingWrite(cdc)) {
         executor.execute(() -> removeIfUnmodifiedInternal(cdc));
      } else {
         removeIfUnmodifiedInternal(cdc);
      }
   }

   private void removeIfUnmodifiedInternal(CacheDecodeContext cdc) {
      cdc.cache().getCacheEntryAsync(cdc.key)
            .whenComplete((entry, throwable) -> handleGetForRemoveIfUnmodified(cdc, entry, throwable));
   }

   private void handleGetForRemoveIfUnmodified(CacheDecodeContext cdc, CacheEntry<byte[], byte[]> entry, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else if (entry != null) {
         byte[] prev = entry.getValue();
         NumericVersion streamVersion = new NumericVersion(cdc.params.streamVersion);
         if (entry.getMetadata().version().equals(streamVersion)) {
            cdc.cache().removeAsync(cdc.key, prev).whenComplete((removed, throwable2) -> {
               if (throwable2 != null) {
                  writeException(cdc, throwable2);
               } else if (removed) {
                  writeSuccess(cdc, prev);
               } else {
                  writeNotExecuted(cdc, prev);
               }
            });
         } else {
            writeNotExecuted(cdc, prev);
         }
      } else {
         writeNotExist(cdc);
      }
   }

   void clear(CacheDecodeContext cdc) {
      if (isBlockingWrite(cdc)) {
         executor.execute(() -> clearInternal(cdc));
      } else {
         clearInternal(cdc);
      }
   }

   private void clearInternal(CacheDecodeContext cdc) {
      cdc.cache().clearAsync().whenComplete((nil, throwable) -> {
         if (throwable != null) {
            writeException(cdc, throwable);
         } else {
            writeSuccess(cdc, null);
         }
      });
   }

   void putAll(CacheDecodeContext cdc) {
      if (isBlockingWrite(cdc)) {
         executor.execute(() -> putAllInternal(cdc));
      } else {
         putAllInternal(cdc);
      }
   }

   private void putAllInternal(CacheDecodeContext cdc) {
      cdc.cache().putAllAsync(cdc.operationContext(), cdc.buildMetadata())
            .whenComplete((nil, throwable) -> handlePutAll(cdc, throwable));
   }

   private void handlePutAll(CacheDecodeContext cdc, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else {
         writeSuccess(cdc, null);
      }
   }

   void getAll(CacheDecodeContext cdc) {
      if (isBlockingRead(cdc, server.getCacheInfo(cdc))) {
         executor.execute(() -> getAllInternal(cdc));
      } else {
         getAllInternal(cdc);
      }
   }

   private void getAllInternal(CacheDecodeContext cdc) {
      cdc.cache().getAllAsync(cdc.operationContext())
            .whenComplete((map, throwable) -> handleGetAll(cdc, map, throwable));
   }

   private void handleGetAll(CacheDecodeContext cdc, Map<byte[], byte[]> map, Throwable throwable) {
      if (throwable != null) {
         writeException(cdc, throwable);
      } else {
         writeResponse(new GetAllResponse(cdc.header.version, cdc.header.messageId, cdc.header.cacheName,
               cdc.header.clientIntel, cdc.header.topologyId, map));
      }
   }

   void size(CacheDecodeContext cdc) {
      executor.execute(() -> sizeInternal(cdc));
   }

   private void sizeInternal(CacheDecodeContext cdc) {
      HotRodHeader h = cdc.header;
      try {
         AdvancedCache<byte[], byte[]> cache = cdc.cache();
         writeResponse(new SizeResponse(h.version, h.messageId, h.cacheName,
               h.clientIntel, h.topologyId, cache.size()));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   void bulkGet(CacheDecodeContext cdc) {
      executor.execute(() -> bulkGetInternal(cdc));
   }

   private void bulkGetInternal(CacheDecodeContext cdc) {
      try {
         AdvancedCache<byte[], byte[]> cache = cdc.cache();
         int size = (int) cdc.operationDecodeContext;
         if (trace) {
            log.tracef("About to create bulk response count = %d", size);
         }
         HotRodHeader h = cdc.header;
         writeResponse(new BulkGetResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               h.topologyId, size, cache.entrySet()));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   public void bulkGetKeys(CacheDecodeContext cdc) {
      executor.execute(() -> bulkGetKeysInternal(cdc));
   }

   private void bulkGetKeysInternal(CacheDecodeContext cdc) {
      try {
         int scope = (int) cdc.operationDecodeContext;
         if (trace) {
            log.tracef("About to create bulk get keys response scope = %d", scope);
         }
         HotRodHeader h = cdc.header;
         writeResponse(new BulkGetKeysResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               h.topologyId, scope, cdc.cache().keySet().iterator()));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   void query(CacheDecodeContext cdc) {
      executor.execute(() -> queryInternal(cdc));
   }

   private void queryInternal(CacheDecodeContext cdc) {
      try {
         byte[] queryResult = server.query(cdc.cache(), (byte[]) cdc.operationDecodeContext);
         HotRodHeader h = cdc.header;
         writeResponse(new QueryResponse(h.version, h.messageId, h.cacheName, h.clientIntel, h.topologyId, queryResult));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   void addClientListener(CacheDecodeContext cdc) {
      executor.execute(() -> addClientListenerInternal(cdc));
   }

   private void addClientListenerInternal(CacheDecodeContext cdc) {
      try {
         ClientListenerRequestContext clientContext = (ClientListenerRequestContext) cdc.operationDecodeContext;
         listenerRegistry.addClientListener(cdc.decoder, channel, cdc.header, clientContext.getListenerId(),
               cdc.cache(), clientContext.isIncludeCurrentState(),
               clientContext.getFilterFactoryInfo().orElse(null),
               clientContext.getConverterFactoryInfo().orElse(null),
               clientContext.isUseRawData(), clientContext.getListenerInterests());
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   void removeClientListener(CacheDecodeContext cdc) {
      executor.execute(() -> removeClientListenerInternal(cdc));
   }

   private void removeClientListenerInternal(CacheDecodeContext cdc) {
      try {
         byte[] listenerId = (byte[]) cdc.operationDecodeContext;
         if (server.getClientListenerRegistry().removeClientListener(listenerId, cdc.cache())) {
            writeResponse(cdc.decoder.createSuccessResponse(cdc.header, null));
         } else {
            writeResponse(cdc.decoder.createNotExecutedResponse(cdc.header, null));
         }
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   void iterationStart(CacheDecodeContext cdc) {
      executor.execute(() -> iterationStartInternal(cdc));
   }

   private void iterationStartInternal(CacheDecodeContext cdc) {
      try {
         IterationStartRequest iterationStart = (IterationStartRequest) cdc.operationDecodeContext;

         Optional<BitSet> optionBitSet;
         if (iterationStart.getOptionBitSet().isPresent()) {
            optionBitSet = Optional.of(BitSet.valueOf(iterationStart.getOptionBitSet().get()));
         } else {
            optionBitSet = Optional.empty();
         }
         String iterationId = server.getIterationManager().start(cdc.cache(), optionBitSet,
               iterationStart.getFactory(), iterationStart.getBatch(), iterationStart.isMetadata());
         HotRodHeader h = cdc.header;
         writeResponse(new IterationStartResponse(h.version, h.messageId, h.cacheName,
               h.clientIntel, h.topologyId, iterationId));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   void iterationNext(CacheDecodeContext cdc) {
      executor.execute(() -> iterationNextInternal(cdc));
   }

   private void iterationNextInternal(CacheDecodeContext cdc) {
      try {
         String iterationId = (String) cdc.operationDecodeContext;
         IterableIterationResult iterationResult = server.getIterationManager().next(cdc.cache().getName(), iterationId);
         HotRodHeader h = cdc.header;
         writeResponse(new IterationNextResponse(h.version, h.messageId, h.cacheName,
               h.clientIntel, h.topologyId, iterationResult));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   void iterationEnd(CacheDecodeContext cdc) {
      executor.execute(() -> iterationEndInternal(cdc));
   }

   private void iterationEndInternal(CacheDecodeContext cdc) {
      try {
         String iterationId = (String) cdc.operationDecodeContext;
         boolean removed = server.getIterationManager().close(cdc.cache().getName(), iterationId);
         HotRodHeader h = cdc.header;
         writeResponse(new EmptyResponse(h.version, h.messageId, h.cacheName, h.clientIntel,
               HotRodOperation.ITERATION_END,
               removed ? OperationStatus.Success : OperationStatus.InvalidIteration, h.topologyId));
      } catch (Throwable t) {
         writeException(cdc, t);
      }
   }

   public void putStream(CacheDecodeContext cdc) {
      ByteBuf buf = (ByteBuf) cdc.operationDecodeContext;
      try {
         byte[] bytes = new byte[buf.readableBytes()];
         buf.readBytes(bytes);
         cdc.operationDecodeContext = bytes;
         long version = cdc.params.streamVersion;
         if (version == 0) { // Normal put
            put(cdc);
         } else if (version < 0) { // putIfAbsent
            putIfAbsent(cdc);
         } else { // versioned replace
            replaceIfUnmodified(cdc);
         }
      } finally {
         buf.release();
      }
   }
}
