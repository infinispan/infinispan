package org.infinispan.server.resp.commands.list.blocking;

import java.lang.invoke.MethodHandles;
import java.util.ArrayDeque;
import java.util.ArrayList;
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
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.filter.EventListenerConverter;
import org.infinispan.server.resp.filter.EventListenerKeysFilter;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.server.resp.meta.ClientMetadata;
import org.infinispan.server.resp.serialization.ResponseWriter;
import org.infinispan.server.resp.tx.TransactionContext;

import io.netty.channel.ChannelHandlerContext;

/**
 *  Derogating to the command documentation, when multiple client are blocked
 *  on a BLPOP, the order in which they will be served is unspecified.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/blpop/">BLPOP</a>
 */
public abstract class AbstractBlockingPop extends RespCommand implements Resp3Command {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   public AbstractBlockingPop(int arity, int firstKeyPos, int lastKeyPos, int steps) {
      super(arity, firstKeyPos, lastKeyPos, steps);
   }

   abstract PopConfiguration parseArguments(Resp3Handler handler, List<byte[]> arguments);

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedMultimapListCache<byte[], byte[]> listMultimap = handler.getListMultimap();
      PopConfiguration configuration = parseArguments(handler, arguments);
      if (configuration == null) {
         return handler.myStage();
      }

      // If all the keys are empty or null, create a listener
      // otherwise return the left value of the first non-empty list
      var pollStage = pollAllKeys(listMultimap, configuration);

      // Running blocking pop from EXEC should not block.
      // In this case, we just return whatever the polling has returned and do not install the listener.
      if (TransactionContext.isInTransactionContext(ctx)) {
         return handler.stageToReturn(pollStage, ctx, ResponseWriter.ARRAY_BULK_STRING);
      }

      // If no value returned, we need subscribers
      return handler.stageToReturn(pollStage.thenCompose(v -> {
         // addSubscriber call can rise exception that needs to be reported
         // as error
         return (v != null && !v.isEmpty())
               ? CompletableFuture.completedFuture(v)
               : addSubscriber(configuration, handler);
      }), ctx, ResponseWriter.ARRAY_BULK_STRING);
   }

   private CompletableFuture<Collection<byte[]>> addSubscriber(PopConfiguration configuration, Resp3Handler handler) {
      if (log.isTraceEnabled()) {
         log.tracef("Subscriber for keys: " + configuration.keys());
      }
      AdvancedCache<byte[], Object> cache = handler.typedCache(null);
      DataConversion vc = cache.getValueDataConversion();
      PubSubListener pubSubListener = new PubSubListener(handler, cache, configuration);
      EventListenerKeysFilter filter = new EventListenerKeysFilter(configuration.keys().stream());
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
         pubSubListener.startTimer(configuration.timeout());
         pubSubListener.synchronizer.onListenerAdded();
      });
      ClientMetadata metadata = handler.respServer().metadataRepository().client();
      metadata.incrementBlockedClients();
      metadata.recordBlockedKeys(configuration.keys().size());
      pubSubListener.getFuture().whenComplete((ignore, t) -> {
         metadata.decrementBlockedClients();
         metadata.recordBlockedKeys(-configuration.keys().size());
      });
      return pubSubListener.getFuture();
   }

   private static CompletionStage<Collection<byte[]>> pollAllKeys(
         EmbeddedMultimapListCache<byte[], byte[]> listMultimap, PopConfiguration configuration) {
      var pollStage = pollKeyValue(listMultimap, configuration.key(0), configuration);
      for (int i = 1; i < configuration.keys().size(); ++i) {
         var keyChannel = configuration.key(i);
         pollStage = pollStage.thenCompose(
               v -> (v == null || v.isEmpty())
                     ? pollKeyValue(listMultimap, keyChannel, configuration)
                     : CompletableFuture.completedFuture(v));
      }
      return pollStage;
   }

   static CompletionStage<Collection<byte[]>> pollKeyValue(EmbeddedMultimapListCache<byte[], byte[]> mmList,
                                                           byte[] key, PopConfiguration configuration) {
      var cs = configuration.isHead() ? mmList.pollFirst(key, configuration.count()) : mmList.pollLast(key, configuration.count());
      return cs
            .thenApply(v -> {
               if (v == null || v.isEmpty())
                  return null;

               List<byte[]> res = new ArrayList<>(1 + v.size());
               res.add(key);
               res.addAll(v);
               return res;
            });
   }

   @Listener(clustered = true)
   public static class PubSubListener {
      private final AdvancedCache<byte[], Object> cache;
      private volatile ScheduledFuture<?> scheduledTimer;
      private final Resp3Handler handler;
      private final PollListenerSynchronizer synchronizer;

      private PubSubListener(Resp3Handler handler, AdvancedCache<byte[], Object> cache, PopConfiguration configuration) {
         this.cache = cache;
         this.handler = handler;
         this.synchronizer = new PollListenerSynchronizer(handler.getListMultimap(), configuration);

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
      private final BiConsumer<? super Collection<byte[]>, ? super Throwable> whenCompleteConsumer;
      private volatile boolean canPollJustEventKey;
      private final PopConfiguration configuration;

      private PollListenerSynchronizer(EmbeddedMultimapListCache<byte[], byte[]> multimapList, PopConfiguration configuration) {
         keyQueue = new ArrayDeque<>();
         resultFuture = new CompletableFuture<>();
         this.multimapList = multimapList;
         this.configuration = configuration;
         whenCompleteConsumer = (v, t) -> {
            if (t != null) {
               // Between installing the listener and doing the first poll, an entry was created.
               // When trying to poll, we identify it is not a list element.
               // We do not complete the future in this case, we leave it until the timeout elapses.
               if (!RespUtil.isWrongTypeError(t)) {
                  resultFuture.completeExceptionally(t);
               }
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
            AbstractBlockingPop.pollKeyValue(multimapList, (byte[]) key, configuration).whenComplete(whenCompleteConsumer);
         } else {
            AbstractBlockingPop.pollAllKeys(multimapList, configuration).whenComplete(whenCompleteConsumer);
         }
      }

      private void onListenerAdded() {
         boolean emptyQueue;
         synchronized (this) {
            emptyQueue = keyQueue.isEmpty();
            keyQueue.offer(this);
         }
         if (emptyQueue) {
            AbstractBlockingPop.pollAllKeys(multimapList, configuration).whenComplete(whenCompleteConsumer);
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
