package org.infinispan.server.resp.commands.list.blocking;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.list.LMOVEM;
import org.infinispan.server.resp.commands.list.LMOVEM.LmovemConfig;
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
public class BLMOVEM extends RespCommand implements Resp3Command {
   private static final byte[] LEFT = "LEFT".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] RIGHT = "RIGHT".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] COUNT = "COUNT".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] EXACTLY = "EXACTLY".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] OBO = "OBO".getBytes(StandardCharsets.US_ASCII);
   private static final byte[] BULK = "BULK".getBytes(StandardCharsets.US_ASCII);

   public BLMOVEM() {
      super(-6, 1, 2, 1,
            AclCategory.WRITE.mask() | AclCategory.LIST.mask() | AclCategory.SLOW.mask() | AclCategory.BLOCKING.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      BlmovemConfig config = parseArguments(handler, arguments);
      if (config == null) return handler.myStage();

      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      CompletionStage<Collection<byte[]>> moveStage = LMOVEM.executeLmovem(listMultimap, config.moveConfig);

      if (TransactionContext.isInTransactionContext(ctx)) {
         return handler.stageToReturn(moveStage, ctx, ResponseWriter.ARRAY_BULK_STRING);
      }

      return handler.stageToReturn(moveStage.thenCompose(v -> {
         if (v != null && !v.isEmpty()) {
            return CompletableFuture.completedFuture(v);
         }
         return addSubscriber(config, handler);
      }), ctx, ResponseWriter.ARRAY_BULK_STRING);
   }

   private BlmovemConfig parseArguments(Resp3Handler handler, List<byte[]> arguments) {
      // BLMOVEM source destination LEFT|RIGHT LEFT|RIGHT timeout [COUNT|EXACTLY count OBO|BULK]
      if (arguments.size() != 5 && arguments.size() != 8) {
         handler.writer().syntaxError();
         return null;
      }

      byte[] source = arguments.get(0);
      byte[] destination = arguments.get(1);

      byte[] srcDir = arguments.get(2);
      byte[] dstDir = arguments.get(3);

      boolean sourceLeft;
      if (RespUtil.isAsciiBytesEquals(LEFT, srcDir)) {
         sourceLeft = true;
      } else if (RespUtil.isAsciiBytesEquals(RIGHT, srcDir)) {
         sourceLeft = false;
      } else {
         handler.writer().syntaxError();
         return null;
      }

      boolean destLeft;
      if (RespUtil.isAsciiBytesEquals(LEFT, dstDir)) {
         destLeft = true;
      } else if (RespUtil.isAsciiBytesEquals(RIGHT, dstDir)) {
         destLeft = false;
      } else {
         handler.writer().syntaxError();
         return null;
      }

      double timeout = ArgumentUtils.toDouble(arguments.get(4));
      if (timeout < 0) {
         handler.writer().mustBePositive("timeout");
         return null;
      }
      long timeoutMillis = (long) (timeout * Duration.ofSeconds(1).toMillis());

      int count = 1;
      boolean exactly = false;
      boolean obo = true;

      if (arguments.size() == 8) {
         byte[] mode = arguments.get(5);
         if (RespUtil.isAsciiBytesEquals(COUNT, mode)) {
            exactly = false;
         } else if (RespUtil.isAsciiBytesEquals(EXACTLY, mode)) {
            exactly = true;
         } else {
            handler.writer().syntaxError();
            return null;
         }

         try {
            count = ArgumentUtils.toInt(arguments.get(6));
         } catch (NumberFormatException e) {
            handler.writer().syntaxError();
            return null;
         }

         if (count <= 0) {
            handler.writer().mustBePositive("count");
            return null;
         }

         byte[] ordering = arguments.get(7);
         if (RespUtil.isAsciiBytesEquals(OBO, ordering)) {
            obo = true;
         } else if (RespUtil.isAsciiBytesEquals(BULK, ordering)) {
            obo = false;
         } else {
            handler.writer().syntaxError();
            return null;
         }
      }

      LmovemConfig moveConfig = new LmovemConfig(source, destination, sourceLeft, destLeft, count, exactly, obo);
      return new BlmovemConfig(moveConfig, timeoutMillis);
   }

   private CompletableFuture<Collection<byte[]>> addSubscriber(BlmovemConfig config, Resp3Handler handler) {
      AdvancedCache<byte[], Object> cache = handler.typedCache(null);
      DataConversion vc = cache.getValueDataConversion();
      MoveListener listener = new MoveListener(handler, cache, config);
      EventListenerKeysFilter filter = new EventListenerKeysFilter(Stream.of(config.moveConfig.source));
      long timeout = config.timeout;
      long deadline = timeout > 0
            ? handler.respServer().getTimeService().expectedEndTime(timeout, TimeUnit.MILLISECONDS)
            : 0;

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

      private MoveListener(Resp3Handler handler, AdvancedCache<byte[], Object> cache, BlmovemConfig config) {
         this.cache = cache;
         this.handler = handler;
         this.synchronizer = new MoveSynchronizer(handler.getListMultimap(), config.moveConfig);

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
         LMOVEM.executeLmovem(multimapList, config).whenComplete(whenCompleteConsumer);
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

   private record BlmovemConfig(LmovemConfig moveConfig, long timeout) {}
}
