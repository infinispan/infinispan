package org.infinispan.server.hotrod;

import static java.lang.String.format;
import static org.infinispan.server.hotrod.ResponseWriting.writeResponse;

import java.util.concurrent.Executor;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.multimap.impl.EmbeddedMultimapCache;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.core.transport.NettyTransport;
import org.infinispan.server.hotrod.logging.Log;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * Handler that performs actual cache operations.
 *
 * @author wburns
 * @since 9.0
 */
public class ContextHandler extends SimpleChannelInboundHandler {
   private final static Log log = LogFactory.getLog(ContextHandler.class, Log.class);

   private final EmbeddedCacheManager cacheManager;
   private final Executor executor;
   private final NettyTransport transport;
   private final HotRodServer server;

   private TransactionRequestProcessor cacheProcessor;
   private CounterRequestProcessor counterProcessor;
   private MultimapRequestProcessor multimapRequestProcessor;
   private TaskRequestProcessor taskRequestProcessor;

   public ContextHandler(EmbeddedCacheManager cacheManager, Executor executor, NettyTransport transport, HotRodServer server) {
      this.cacheManager = cacheManager;
      this.executor = executor;
      this.transport = transport;
      this.server = server;
   }

   @Override
   public void handlerAdded(ChannelHandlerContext ctx) {
      cacheProcessor = new TransactionRequestProcessor(ctx.channel(), executor, server);
      counterProcessor = new CounterRequestProcessor(ctx.channel(), (EmbeddedCounterManager) EmbeddedCounterManagerFactory.asCounterManager(cacheManager), executor, server);
      multimapRequestProcessor = new MultimapRequestProcessor(ctx.channel(), executor);
      taskRequestProcessor = new TaskRequestProcessor(ctx.channel(), executor, server);
   }

   private void initCache(CacheDecodeContext cdc) throws RequestParsingException {
      cdc.resource = cache(cdc);
   }

   private void initMultimap(CacheDecodeContext cdc) throws RequestParsingException {
      cdc.resource = new EmbeddedMultimapCache(cache(cdc));
   }

   public AdvancedCache<byte[], byte[]> cache(CacheDecodeContext cdc) throws RequestParsingException {
      String cacheName = cdc.header.cacheName;
      // Try to avoid calling cacheManager.getCacheNames() if possible, since this creates a lot of unnecessary garbage
      AdvancedCache<byte[], byte[]> cache = server.getKnownCache(cacheName);
      if (cache == null) {
         cache = obtainCache(cdc.header, cacheName);
      }
      cache = cdc.decoder.getOptimizedCache(cdc.header, cache, server.getCacheConfiguration(cacheName));
      if (cdc.subject != null) {
         cache = cache.withSubject(cdc.subject);
      }
      return cache;
   }

   private AdvancedCache<byte[], byte[]> obtainCache(HotRodHeader header, String cacheName) throws RequestParsingException {
      AdvancedCache<byte[], byte[]> cache;// Talking to the wrong cache are really request parsing errors
      // and hence should be treated as client errors
      InternalCacheRegistry icr = cacheManager.getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      if (icr.isPrivateCache(cacheName)) {
         throw new RequestParsingException(
               format("Remote requests are not allowed to private caches. Do no send remote requests to cache '%s'", cacheName),
               header.version, header.messageId);
      } else if (icr.internalCacheHasFlag(cacheName, InternalCacheRegistry.Flag.PROTECTED)) {
         // We want to make sure the cache access is checked everytime, so don't store it as a "known" cache. More
         // expensive, but these caches should not be accessed frequently
         cache = server.getCacheInstance(cacheName, cacheManager, true, false);
      } else if (!cacheName.isEmpty() && !cacheManager.getCacheNames().contains(cacheName)) {
         throw new CacheNotFoundException(
               format("Cache with name '%s' not found amongst the configured caches", cacheName),
               header.version, header.messageId);
      } else {
         cache = server.getCacheInstance(cacheName, cacheManager, true, true);
      }
      return cache;
   }

   @Override
   public boolean acceptInboundMessage(Object msg) throws Exception {
      // Faster than netty matcher
      return msg.getClass() == CacheDecodeContext.class;
   }

   @Override
   protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
      CacheDecodeContext cdc = (CacheDecodeContext) msg;
      HotRodHeader h = cdc.header;
      switch (h.op) {
         case PUT:
            initCache(cdc);
            cacheProcessor.put(cdc);
            break;
         case PUT_IF_ABSENT:
            initCache(cdc);
            cacheProcessor.putIfAbsent(cdc);
            break;
         case REPLACE:
            initCache(cdc);
            cacheProcessor.replace(cdc);
            break;
         case REPLACE_IF_UNMODIFIED:
            initCache(cdc);
            cacheProcessor.replaceIfUnmodified(cdc);
            break;
         case CONTAINS_KEY:
            initCache(cdc);
            cacheProcessor.containsKey(cdc);
            break;
         case GET:
         case GET_WITH_VERSION:
            initCache(cdc);
            cacheProcessor.get(cdc);
            break;
         case GET_STREAM:
         case GET_WITH_METADATA:
            initCache(cdc);
            cacheProcessor.getKeyMetadata(cdc);
            break;
         case REMOVE:
            initCache(cdc);
            cacheProcessor.remove(cdc);
            break;
         case REMOVE_IF_UNMODIFIED:
            initCache(cdc);
            cacheProcessor.removeIfUnmodified(cdc);
            break;
         case PING:
            cache(cdc); // we need to throw an exception when this cache is inaccessible
            writeResponse(ctx.channel(), new EmptyResponse(h.version, h.messageId, h.cacheName,
                  h.clientIntel, HotRodOperation.PING, OperationStatus.Success, h.topologyId));
            break;
         case STATS:
            writeResponse(ctx.channel(), cdc.decoder.createStatsResponse(cdc, cache(cdc).getStats(), transport));
            break;
         case CLEAR:
            initCache(cdc);
            cacheProcessor.clear(cdc);
            break;
         case EXEC:
            initCache(cdc);
            taskRequestProcessor.exec(cdc);
            break;
         case PUT_ALL:
            initCache(cdc);
            cacheProcessor.putAll(cdc);
            break;
         case GET_ALL:
            initCache(cdc);
            cacheProcessor.getAll(cdc);
            break;
         case PUT_STREAM:
            initCache(cdc);
            cacheProcessor.putStream(cdc);
            break;
         case SIZE:
            initCache(cdc);
            cacheProcessor.size(cdc);
            break;
         case BULK_GET:
            initCache(cdc);
            cacheProcessor.bulkGet(cdc);
            break;
         case BULK_GET_KEYS:
            initCache(cdc);
            cacheProcessor.bulkGetKeys(cdc);
            break;
         case QUERY:
            initCache(cdc);
            cacheProcessor.query(cdc);
            break;
         case ADD_CLIENT_LISTENER:
            initCache(cdc);
            cacheProcessor.addClientListener(cdc);
            break;
         case REMOVE_CLIENT_LISTENER:
            initCache(cdc);
            cacheProcessor.removeClientListener(cdc);
            break;
         case ITERATION_START:
            initCache(cdc);
            cacheProcessor.iterationStart(cdc);
            break;
         case ITERATION_NEXT:
            initCache(cdc);
            cacheProcessor.iterationNext(cdc);
            break;
         case ITERATION_END:
            initCache(cdc);
            cacheProcessor.iterationEnd(cdc);
            break;
         case ROLLBACK_TX:
            initCache(cdc);
            cacheProcessor.rollbackTransaction(cdc);
            break;
         case PREPARE_TX:
            initCache(cdc);
            cacheProcessor.prepareTransaction(cdc);
            break;
         case COMMIT_TX:
            initCache(cdc);
            cacheProcessor.commitTransaction(cdc);
            break;
         case COUNTER_CREATE:
            counterProcessor.createCounter(cdc);
            break;
         case COUNTER_GET_CONFIGURATION:
            counterProcessor.getCounterConfiguration(cdc);
            break;
         case COUNTER_IS_DEFINED:
            counterProcessor.isCounterDefined(cdc);
            break;
         case COUNTER_ADD_AND_GET:
            counterProcessor.counterAddAndGet(cdc);
            break;
         case COUNTER_RESET:
            counterProcessor.counterReset(cdc);
            break;
         case COUNTER_GET:
            counterProcessor.counterGet(cdc);
            break;
         case COUNTER_CAS:
            counterProcessor.counterCompareAndSwap(cdc);
            break;
         case COUNTER_REMOVE:
            counterProcessor.counterRemove(cdc);
            break;
         case COUNTER_GET_NAMES:
            counterProcessor.getCounterNames(cdc);
            break;
         case COUNTER_REMOVE_LISTENER:
            counterProcessor.removeCounterListener(cdc);
            break;
         case COUNTER_ADD_LISTENER:
            counterProcessor.addCounterListener(cdc);
            break;
         case CONTAINS_KEY_MULTIMAP:
            initMultimap(cdc);
            multimapRequestProcessor.containsKey(cdc);
            break;
         case GET_MULTIMAP:
            initMultimap(cdc);
            multimapRequestProcessor.get(cdc);
            break;
         case GET_MULTIMAP_WITH_METADATA:
            initMultimap(cdc);
            multimapRequestProcessor.getWithMetadata(cdc);
            break;
         case PUT_MULTIMAP:
            initMultimap(cdc);
            multimapRequestProcessor.put(cdc);
            break;
         case REMOVE_MULTIMAP:
            initMultimap(cdc);
            multimapRequestProcessor.removeKey(cdc);
            break;
         case REMOVE_ENTRY_MULTIMAP:
            initMultimap(cdc);
            multimapRequestProcessor.removeEntry(cdc);
            break;
         case SIZE_MULTIMAP:
            initMultimap(cdc);
            multimapRequestProcessor.size(cdc);
            break;
         case CONTAINS_ENTRY_MULTIMAP:
            initMultimap(cdc);
            multimapRequestProcessor.containsEntry(cdc);
            break;
         case CONTAINS_VALUE_MULTIMAP:
            initMultimap(cdc);
            multimapRequestProcessor.containsValue(cdc);
            break;
         default:
            throw new IllegalArgumentException("Unsupported operation invoked: " + cdc.header.op);
      }
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      super.channelActive(ctx);
      log.tracef("Channel %s became active", ctx.channel());
      server.getClientListenerRegistry().findAndWriteEvents(ctx.channel());
      server.getClientCounterNotificationManager().channelActive(ctx.channel());
   }

   @Override
   public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      super.channelWritabilityChanged(ctx);
      log.tracef("Channel %s writability changed", ctx.channel());
      server.getClientListenerRegistry().findAndWriteEvents(ctx.channel());
      server.getClientCounterNotificationManager().channelActive(ctx.channel());
   }
}
