package org.infinispan.server.resp.commands.list.blocking;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.infinispan.AdvancedCache;
import org.infinispan.encoding.DataConversion;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.list.AbstractLMOVEM;
import org.infinispan.server.resp.filter.EventListenerConverter;
import org.infinispan.server.resp.filter.EventListenerKeysFilter;
import org.infinispan.server.resp.meta.ClientMetadata;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.tx.TransactionContext;

import io.netty.channel.ChannelHandlerContext;

/**
 * BLMOVEM
 *
 * @see <a href="https://redis.io/commands/blmovem/">BLMOVEM</a>
 * @since 16.3
 */
public class BLMOVEM extends AbstractLMOVEM implements Resp3Command {
   public BLMOVEM() {
      super(-6,
            AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask() | AclCategory.BLOCKING.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      LmovemConfig config = parseArguments(handler, arguments, true);
      if (config == null) return handler.myStage();

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<Collection<byte[]>> moveStage = executeLmovem("BLMOVEM", listMultimap, config);

      if (TransactionContext.isInTransactionContext(ctx)) {
         return handler.stageToReturn(moveStage, ctx, ResponseWriter.ARRAY_BULK_STRING_OR_NIL);
      }

      return handler.stageToReturn(moveStage.thenCompose(v -> {
         if (v != null && !v.isEmpty()) {
            return CompletableFuture.completedFuture(v);
         }
         return addSubscriber(config, handler);
      }), ctx, ResponseWriter.ARRAY_BULK_STRING_OR_NIL);
   }

   private CompletableFuture<Collection<byte[]>> addSubscriber(LmovemConfig config, Resp3Handler handler) {
      AdvancedCache<byte[], Object> cache = handler.typedCache(null);
      DataConversion vc = cache.getValueDataConversion();
      MoveListener listener = new MoveListener(handler, cache, config);
      EventListenerKeysFilter filter = new EventListenerKeysFilter(Stream.of(config.source()));
      long timeout = config.timeout();
      long deadline = AbstractBlockingPop.deadline(handler, timeout);

      CompletionStage<Void> addListenerStage = cache.addListenerAsync(listener, filter,
            new EventListenerConverter<Object, Object, byte[]>(vc));
      addListenerStage.whenComplete((ignore, t) -> {
         if (t != null) {
            listener.synchronizer.resultFuture.completeExceptionally(t);
            return;
         }
         if (timeout > 0) {
            long remaining = handler.respServer().getTimeService().remainingTime(deadline, TimeUnit.MILLISECONDS);
            if (remaining <= 0) {
               cache.removeListenerAsync(listener);
               listener.synchronizer.resultFuture.complete(null);
               return;
            }
            listener.startTimer(remaining);
         }
         listener.synchronizer.onListenerAdded();
      });

      ClientMetadata metadata = handler.respServer().metadataRepository().client();
      metadata.incrementBlockedClients();
      metadata.recordBlockedKeys(1);
      listener.getFuture().whenComplete((ignore, t) -> {
         metadata.decrementBlockedClients();
         metadata.recordBlockedKeys(-1);
      });
      return listener.getFuture();
   }

   @Listener(clustered = true)
   public static class MoveListener {
      private final AdvancedCache<byte[], Object> cache;
      private volatile ScheduledFuture<?> scheduledTimer;
      private final Resp3Handler handler;
      private final MoveSynchronizer synchronizer;

      private MoveListener(Resp3Handler handler, AdvancedCache<byte[], Object> cache, LmovemConfig config) {
         this.cache = cache;
         this.handler = handler;
         this.synchronizer = new MoveSynchronizer(handler.getListMultimap(), config);

         synchronizer.resultFuture.whenComplete((ignore_v, ignore_t) -> {
            deleteTimer();
            cache.removeListenerAsync(this);
         });
      }

      public CompletableFuture<Collection<byte[]>> getFuture() {
         return synchronizer.resultFuture;
      }

      private void startTimer(long timeout) {
         deleteTimer();
         scheduledTimer = (timeout > 0) ? handler.getScheduler().schedule(() -> {
            cache.removeListenerAsync(this);
            synchronizer.resultFuture.complete(null);
         }, timeout, TimeUnit.MILLISECONDS) : null;
      }

      private void deleteTimer() {
         if (scheduledTimer != null)
            scheduledTimer.cancel(true);
         scheduledTimer = null;
      }

      @CacheEntryCreated
      @CacheEntryModified
      public void onEvent(CacheEntryEvent<Object, Object> entryEvent) {
         try {
            if (entryEvent.getValue() instanceof ListBucket) {
               synchronizer.onEvent();
            }
         } catch (Exception ex) {
            synchronizer.resultFuture.completeExceptionally(ex);
         }
      }
   }

   static class MoveSynchronizer {
      private final ArrayDeque<Object> eventQueue;
      final CompletableFuture<Collection<byte[]>> resultFuture;
      private final EmbeddedMultimapListCache<byte[], byte[]> multimapList;
      private final LmovemConfig config;
      private final BiConsumer<? super Collection<byte[]>, ? super Throwable> whenCompleteConsumer;
      private volatile boolean listenerAdded;

      MoveSynchronizer(EmbeddedMultimapListCache<byte[], byte[]> multimapList, LmovemConfig config) {
         this.eventQueue = new ArrayDeque<>();
         this.resultFuture = new CompletableFuture<>();
         this.multimapList = multimapList;
         this.config = config;
         this.whenCompleteConsumer = (v, t) -> {
            if (t != null) {
               if (!RespUtil.isWrongTypeError(t)) {
                  resultFuture.completeExceptionally(t);
               }
            } else if (v != null && !v.isEmpty()) {
               resultFuture.complete(v);
            } else {
               boolean hasMore;
               synchronized (this) {
                  eventQueue.poll();
                  hasMore = !eventQueue.isEmpty();
               }
               if (hasMore) {
                  tryMove();
               }
            }
         };
      }

      private void tryMove() {
         AbstractLMOVEM.executeLmovem("BLMOVEM", multimapList, config).whenComplete(whenCompleteConsumer);
      }

      void onListenerAdded() {
         boolean emptyQueue;
         synchronized (this) {
            emptyQueue = eventQueue.isEmpty();
            listenerAdded = true;
            eventQueue.offer(this);
         }
         if (emptyQueue) {
            tryMove();
         }
      }

      void onEvent() {
         boolean emptyQueue;
         synchronized (this) {
            emptyQueue = eventQueue.isEmpty();
            eventQueue.offer(Boolean.TRUE);
         }
         if (emptyQueue) {
            tryMove();
         }
      }
   }
}
