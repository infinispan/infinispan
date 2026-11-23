package org.infinispan.server.resp.commands.list.blocking;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import org.infinispan.AdvancedCache;
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
 * Base class for the blocking pop commands. The data structure being popped from is defined by a {@link Source}.
 * <p>
 * Derogating to the command documentation, when multiple client are blocked
 * on a BLPOP, the order in which they will be served is unspecified.
 *
 * @param <R> the type of a successful pop result
 * @see <a href="https://redis.io/commands/blpop/">BLPOP</a>
 * @since 15.0
 */
public abstract class AbstractBlockingPop<R> extends RespCommand implements Resp3Command {
   private static final Log log = Log.getLog(AbstractBlockingPop.class);

   /**
    * Pops from the head or tail of lists. The result is the key followed by the popped values.
    */
   public static final Source<Collection<byte[]>> LIST = new Source<>() {
      @Override
      public CompletionStage<Collection<byte[]>> poll(Resp3Handler handler, byte[] key, PopConfiguration configuration) {
         return pollKeyValue(handler.getListMultimap(), key, configuration);
      }

      @Override
      public boolean isBucket(Object value) {
         return value instanceof ListBucket;
      }

      @Override
      public BiConsumer<? super Collection<byte[]>, ResponseWriter> serializer() {
         return ResponseWriter.ARRAY_BULK_STRING;
      }
   };

   private final Source<R> source;

   protected AbstractBlockingPop(Source<R> source, int arity, int firstKeyPos, int lastKeyPos, int steps, long aclMask) {
      super(arity, firstKeyPos, lastKeyPos, steps, aclMask);
      this.source = source;
   }

   /**
    * Parses the command arguments, writing an error and returning {@code null} when they are invalid.
    */
   protected abstract PopConfiguration parseArguments(Resp3Handler handler, List<byte[]> arguments);

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      PopConfiguration configuration = parseArguments(handler, arguments);
      if (configuration == null) {
         return handler.myStage();
      }

      // If all the keys are empty or null, create a listener
      // otherwise return the value popped from the first non-empty key
      var pollStage = pollAllKeys(source, handler, configuration);

      // Running blocking pop from EXEC should not block.
      // In this case, we just return whatever the polling has returned and do not install the listener.
      if (TransactionContext.isInTransactionContext(ctx)) {
         return handler.stageToReturn(pollStage, ctx, source.serializer());
      }

      // If no value returned, we need subscribers
      return handler.stageToReturn(pollStage.thenCompose(v -> {
         // addSubscriber call can rise exception that needs to be reported
         // as error
         return v != null
               ? CompletableFuture.completedFuture(v)
               : addSubscriber(configuration, handler);
      }), ctx, source.serializer());
   }

   /**
    * Computes the absolute deadline for a timeout, or {@code 0} when no timeout is set. The deadline is recorded
    * before listener installation so that it is relative to when the command was issued, not when the listener is
    * fully installed (which may take time in clustered environments).
    */
   static long deadline(Resp3Handler handler, long timeout) {
      return timeout > 0 ? handler.respServer().getTimeService().expectedEndTime(timeout, TimeUnit.MILLISECONDS) : 0;
   }

   private CompletableFuture<R> addSubscriber(PopConfiguration configuration, Resp3Handler handler) {
      if (log.isTraceEnabled()) {
         log.tracef("Subscriber for keys: %s", configuration.keys());
      }
      AdvancedCache<byte[], Object> cache = handler.typedCache(null);
      DataConversion vc = cache.getValueDataConversion();
      PubSubListener<R> pubSubListener = new PubSubListener<>(handler, cache, source, configuration);
      EventListenerKeysFilter filter = new EventListenerKeysFilter(configuration.keys().stream());
      long timeout = configuration.timeout();
      long deadline = deadline(handler, timeout);
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
         // Start a timer with the remaining time, accounting for time spent installing the listener.
         if (timeout > 0) {
            long remaining = handler.respServer().getTimeService().remainingTime(deadline, TimeUnit.MILLISECONDS);
            if (remaining <= 0) {
               // Timeout expired during listener installation. Complete immediately.
               cache.removeListenerAsync(pubSubListener);
               pubSubListener.synchronizer.resultFuture.complete(null);
               return;
            }
            pubSubListener.startTimer(remaining);
         }
         pubSubListener.synchronizer.onListenerAdded();
      });
      ClientMetadata metadata = handler.respServer().metadataRepository().client();
      metadata.incrementBlockedClients();
      metadata.recordBlockedKeys(configuration.keys().size());
      // Return the dependent future, so the metadata is updated before the reply is written to the client.
      // Dependents of the original future would run in reverse registration order, after the reply.
      return pubSubListener.getFuture().whenComplete((ignore, t) -> {
         metadata.decrementBlockedClients();
         metadata.recordBlockedKeys(-configuration.keys().size());
      });
   }

   private static <R> CompletionStage<R> pollAllKeys(Source<R> source, Resp3Handler handler,
                                                    PopConfiguration configuration) {
      var pollStage = source.poll(handler, configuration.key(0), configuration);
      for (int i = 1; i < configuration.keys().size(); ++i) {
         var keyChannel = configuration.key(i);
         pollStage = pollStage.thenCompose(
               v -> v == null
                     ? source.poll(handler, keyChannel, configuration)
                     : CompletableFuture.completedFuture(v));
      }
      return pollStage;
   }

   private static CompletionStage<Collection<byte[]>> pollKeyValue(EmbeddedMultimapListCache<byte[], byte[]> mmList,
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

   /**
    * The data structure a blocking pop command pops from.
    *
    * @param <R> the type of a successful pop result
    */
   public interface Source<R> {
      /**
       * Pops from the given key.
       *
       * @return a stage with the result, or {@code null} when there was nothing to pop
       */
      CompletionStage<R> poll(Resp3Handler handler, byte[] key, PopConfiguration configuration);

      /**
       * @return {@code true} if a cache entry with this value can be popped from
       */
      boolean isBucket(Object value);

      /**
       * @return the serializer for the reply, which also handles a {@code null} result (timeout)
       */
      BiConsumer<? super R, ResponseWriter> serializer();
   }

   @Listener(clustered = true)
   public static class PubSubListener<R> {
      private final AdvancedCache<byte[], Object> cache;
      private volatile ScheduledFuture<?> scheduledTimer;
      private final Resp3Handler handler;
      private final Predicate<Object> isBucket;
      private final PollListenerSynchronizer<R> synchronizer;

      private PubSubListener(Resp3Handler handler, AdvancedCache<byte[], Object> cache, Source<R> source,
                             PopConfiguration configuration) {
         this.cache = cache;
         this.handler = handler;
         this.isBucket = source::isBucket;
         this.synchronizer = new PollListenerSynchronizer<>(handler, source, configuration);

         synchronizer.resultFuture.whenComplete((ignore_v, ignore_t) -> {
            deleteTimer();
            cache.removeListenerAsync(this);
         });
      }

      public CompletableFuture<R> getFuture() {
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
            if (isBucket.test(entryEvent.getValue())) {
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
    * <p>
    * This class synchronizes the access to a CompletableFuture `resultFuture` so
    * that its final value will be completed either
    * - with value v by an onListenerAdded() call if a not null value is found
    * - otherwise by onEvent(k) call if the referred entry is not null or empty
    *
    */
   public static class PollListenerSynchronizer<R> {
      private final ArrayDeque<Object> keyQueue;
      private final CompletableFuture<R> resultFuture;
      private final Resp3Handler handler;
      private final Source<R> source;
      private final BiConsumer<? super R, ? super Throwable> whenCompleteConsumer;
      private volatile boolean canPollJustEventKey;
      private final PopConfiguration configuration;

      private PollListenerSynchronizer(Resp3Handler handler, Source<R> source, PopConfiguration configuration) {
         keyQueue = new ArrayDeque<>();
         resultFuture = new CompletableFuture<>();
         this.handler = handler;
         this.source = source;
         this.configuration = configuration;
         whenCompleteConsumer = (v, t) -> {
            if (t != null) {
               // Between installing the listener and doing the first poll, an entry was created.
               // When trying to poll, we identify it is not of the expected type.
               // We do not complete the future in this case, we leave it until the timeout elapses.
               if (!RespUtil.isWrongTypeError(t)) {
                  resultFuture.completeExceptionally(t);
               }
            } else if (v != null) {
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
            source.poll(handler, (byte[]) key, configuration).whenComplete(whenCompleteConsumer);
         } else {
            AbstractBlockingPop.pollAllKeys(source, handler, configuration).whenComplete(whenCompleteConsumer);
         }
      }

      private void onListenerAdded() {
         boolean emptyQueue;
         synchronized (this) {
            emptyQueue = keyQueue.isEmpty();
            keyQueue.offer(this);
         }
         if (emptyQueue) {
            AbstractBlockingPop.pollAllKeys(source, handler, configuration).whenComplete(whenCompleteConsumer);
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
