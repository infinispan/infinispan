package org.infinispan.server.resp.commands.list.blocking;

import java.lang.invoke.MethodHandles;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.encoding.DataConversion;
import org.infinispan.multimap.impl.EmbeddedMultimapListCache;
import org.infinispan.multimap.impl.ListBucket;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryModified;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.filter.EventListenerConverter;
import org.infinispan.server.resp.filter.EventListenerKeysFilter;
import org.infinispan.server.resp.logging.Log;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/blpop/
 *       Derogating to the above documentation, when multiple client are blocked
 *       on a BLPOP, the order in which they will be served is unspecified.
 * @since 15.0
 */
public class BPOP extends RespCommand implements Resp3Command {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   protected final boolean isFirst;

   public BPOP(boolean isFirst) {
      super(-3, 1, -2, 1);
      this.isFirst = isFirst;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      var lastKeyIdx = arguments.size() - 1;
      var filterKeys = arguments.subList(0, lastKeyIdx);
      // Using last arg as timeout if it can be a double
      var argTimeout = ArgumentUtils.toDouble(arguments.get(lastKeyIdx));
      if (argTimeout < 0) {
         RespErrorUtil.mustBePositive(handler.allocator());
         return handler.myStage();
      }
      long timeout = (long) (argTimeout * Duration.ofSeconds(1).toMillis());

      // If all the keys are empty or null, create a listener
      // otherwise return the left value of the first non empty list
      var pollStage = pollAllKeys(listMultimap, filterKeys, isFirst);
      // If no value returned, we need subscribers
      return handler.stageToReturn(pollStage.thenCompose(v -> {
         // addSubscriber call can rise exception that needs to be reported
         // as error
         var retStage = (v != null && !v.isEmpty())
               ? CompletableFuture.completedFuture(v)
               : addSubscriber(listMultimap, filterKeys, timeout, handler);
         return retStage;
      }), ctx, Consumers.COLLECTION_BULK_BICONSUMER);
   }

   CompletionStage<Collection<byte[]>> addSubscriber(EmbeddedMultimapListCache<byte[], byte[]> listMultimap,
         List<byte[]> filterKeys, long timeout, Resp3Handler handler) {
      if (log.isTraceEnabled()) {
         log.tracef("Subscriber for keys: " +
               filterKeys.toString());
      }
      AdvancedCache<byte[], Object> cache = handler.typedCache(null);
      DataConversion vc = cache.getValueDataConversion();
      PubSubListener pubSubListener = new PubSubListener(filterKeys, handler, cache, listMultimap, isFirst);
      EventListenerKeysFilter filter = new EventListenerKeysFilter(filterKeys.toArray(byte[][]::new));
      CompletionStage<Void> addListenerStage = cache.addListenerAsync(pubSubListener, filter,
            new EventListenerConverter<Object, Object, byte[]>(vc));
      addListenerStage.whenComplete((ignore, t) -> {
         // If listener fails to install, complete exceptionally pubSubFuture and return
         if (t != null) {
            pubSubListener.synchronizer.resultFuture.completeExceptionally(t);
            return;
         }
         // Listener can lose events during its install, so we need to poll again
         // and if we get values complete the listener future. In case of exception
         // completeExceptionally
         // Start a timer if required
         pubSubListener.startTimer(timeout);
         pubSubListener.synchronizer.onListenerAdded();
      });
      return pubSubListener.getFuture();
   }

   private static CompletionStage<Collection<byte[]>> pollAllKeys(
         EmbeddedMultimapListCache<byte[], byte[]> listMultimap,
         List<byte[]> filterKeys, boolean isFirst) {
      var pollStage = pollKeyValue(listMultimap, filterKeys.get(0), isFirst);
      for (int i = 1; i < filterKeys.size(); ++i) {
         var keyChannel = filterKeys.get(i);
         pollStage = pollStage.thenCompose(
               v -> (v == null || v.isEmpty())
                     ? pollKeyValue(listMultimap, keyChannel, isFirst)
                     : CompletableFuture.completedFuture(v));
      }
      return pollStage;
   }

   static CompletionStage<Collection<byte[]>> pollKeyValue(EmbeddedMultimapListCache<byte[], byte[]> mmList,
         byte[] key, boolean isFirst) {
            var cs = isFirst ? mmList.pollFirst(key, 1) : mmList.pollLast(key, 1);
      return cs
            .thenApply((v) -> (v == null || v.isEmpty())
                  ? null
                  : Arrays.asList(key, v.iterator().next()));
   }

   @Listener(clustered = true)
   public static class PubSubListener {
      private final AdvancedCache<byte[], Object> cache;
      private volatile ScheduledFuture<?> scheduledTimer;
      private final Resp3Handler handler;
      private final PollListenerSynchronizer synchronizer;

      private PubSubListener(List<byte[]> filterKeys, Resp3Handler handler, AdvancedCache<byte[], Object> cache,
            EmbeddedMultimapListCache<byte[], byte[]> mml, boolean isFirst) {
         this.cache = cache;
         this.handler = handler;
         this.synchronizer = new PollListenerSynchronizer(filterKeys, mml, isFirst);

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
               byte[] key = unwrapKey(entryEvent.getKey());
               synchronizer.onEvent(key);
            }
         } catch (Exception ex) {
            synchronizer.resultFuture.completeExceptionally(ex);
         }
      }

      private byte[] unwrapKey(Object key) {
         return key instanceof WrappedByteArray
               ? ((WrappedByteArray) key).getBytes()
               : (byte[]) key;
      }
   }

   /**
    * PollListenerSynchronizer
    *
    * This class synchronizes the access to a CompletableFuture `resultFuture` so
    * that its final value will be completed either
    * - with value v by an onListenerAdded() call if a not null value is found
    * - otherwise by onEvent(k) call if the referred entry is not null or empty
    *
    */
   public static class PollListenerSynchronizer {
      private final ArrayDeque<Object> keyQueue;
      private final CompletableFuture<Collection<byte[]>> resultFuture;
      private final EmbeddedMultimapListCache<byte[], byte[]> multimapList;
      private final List<byte[]> keys;
      private final BiConsumer<? super Collection<byte[]>, ? super Throwable> whenCompleteConsumer;
      private volatile boolean canPollJustEventKey;
      private final boolean isFirst;

      private PollListenerSynchronizer(List<byte[]> keys, EmbeddedMultimapListCache<byte[], byte[]> multimapList, boolean isFirst) {
         keyQueue = new ArrayDeque<Object>();
         resultFuture = new CompletableFuture<Collection<byte[]>>();
         this.multimapList = multimapList;
         this.keys = keys;
         this.isFirst = isFirst;
         whenCompleteConsumer = (v, t) -> {
            if (t != null) {
               resultFuture.completeExceptionally(t);
            } else if (v != null && !v.isEmpty()) {
               resultFuture.complete(v);
            } else {
               Object key;
               synchronized (this) {
                  if (keyQueue.poll() == this) {
                     canPollJustEventKey = true;
                  }
                  key = keyQueue.peek();
               }
               if (key != null) {
                  runPoll(key);
               }
            }
         };
      }

      private void runPoll(Object key) {
         if (canPollJustEventKey && key != this) {
            BPOP.pollKeyValue(multimapList, (byte[])key, isFirst).whenComplete(whenCompleteConsumer);
         } else {
            BPOP.pollAllKeys(multimapList, keys, isFirst).whenComplete(whenCompleteConsumer);
         }
      }

      private void onListenerAdded() {
         boolean emptyQueue;
         synchronized (this) {
            emptyQueue = keyQueue.isEmpty();
            keyQueue.offer(this);
         }
         if (emptyQueue) {
            BPOP.pollAllKeys(multimapList, keys, isFirst).whenComplete(whenCompleteConsumer);
         }
      }

      private void onEvent(byte[] key) {
         boolean emptyQueue;
         synchronized (this) {
            emptyQueue = keyQueue.isEmpty();
            keyQueue.offer(key);
         }
         if (emptyQueue) {
            runPoll(key);
         }
      }
   }
}
