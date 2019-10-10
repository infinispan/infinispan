package org.infinispan.reactive.publisher.impl;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import org.infinispan.commons.util.IntSet;
import org.infinispan.reactive.publisher.impl.commands.batch.KeyPublisherResponse;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.exceptions.MissingBackpressureException;
import io.reactivex.internal.fuseable.SimplePlainQueue;
import io.reactivex.internal.queue.SpscArrayQueue;
import io.reactivex.internal.queue.SpscLinkedArrayQueue;
import io.reactivex.internal.subscriptions.EmptySubscription;
import io.reactivex.internal.subscriptions.SubscriptionHelper;
import io.reactivex.internal.util.BackpressureHelper;
import net.jcip.annotations.GuardedBy;

/**
 * A Publisher implementation that handles the submission and response handling of an arbitrary amount of address
 * segments. This class will based upon upstream requests send a request to the target address until has retrieved
 * enough entries to satisfy the request threshold. When a given address can no longer return any entries this
 * subscription will try to process the next address/segment combination until it can no longer find any more
 * address/segment targets.
 * <p>
 * Note that this publisher can only be subscribed to by one subscriber (more than 1 subscriber will cause issues)
 * <p>
 * Many methods may have a GuardedBy with "requestors" in it. This means the requestor should only ever invoke this
 * method if they were able to increment the requestors variable from 0 to 1. If requestors is a different value the
 * thread should return as soon as possible.
 * @param <R>
 */
class InnerPublisherSubscription<K, I, R> extends AtomicLong implements Publisher<R>, Subscription {
   protected final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   protected final static boolean trace = log.isTraceEnabled();

   private final ClusterPublisherManagerImpl<K, ?>.SubscriberHandler<I, R> parent;
   private final SimplePlainQueue<R> queue;
   private final Supplier<Map.Entry<Address, IntSet>> supplier;
   private final int batchSize;
   private final Map<Address, Set<K>> excludedKeys;
   private final int topologyId;

   // The upstream subscriber we will send values to
   private AtomicReference<Subscriber<? super R>> subscriber = new AtomicReference<>();
   // This variable is here to track how many requests have been invoked, by incrementing it. If a request changes this
   // from 0 to 1, this request may begin processing entries. When the values are processed this variable should be
   // decremented and if the value is now 0 the invoker may return otherwise it must repeat until the requestors becomes 0.
   // This is important so that only a single caller can be adding things to the upstream subscriber.
   private final AtomicInteger requestors = new AtomicInteger();

   // The current address and segments we are processing or null if another one should be acquired
   private volatile Map.Entry<Address, IntSet> currentTarget;
   // whether this subscription was cancelled by a caller (means we can stop processing)
   private volatile boolean cancelled;
   // whether the initial request was already sent or not (if so then a next command is used)
   private volatile boolean alreadyCreated;

   InnerPublisherSubscription(ClusterPublisherManagerImpl<K, ?>.SubscriberHandler<I, R> parent,
         int batchSize, Supplier<Map.Entry<Address, IntSet>> supplier, Map<Address, Set<K>> excludedKeys, int topologyId) {
      this.parent = parent;
      this.queue = queueToUse(parent.publisher.shouldTrackKeys, batchSize);
      this.supplier = supplier;
      this.batchSize = batchSize;
      this.excludedKeys = excludedKeys;
      this.topologyId = topologyId;
   }

   InnerPublisherSubscription(ClusterPublisherManagerImpl<K, ?>.SubscriberHandler<I, R> parent,
         int batchSize, Supplier<Map.Entry<Address, IntSet>> supplier, Map<Address, Set<K>> excludedKeys, int topologyId,
         Map.Entry<Address, IntSet> specificTarget) {
      this(parent, batchSize, supplier, excludedKeys, topologyId);

      this.currentTarget = Objects.requireNonNull(specificTarget);
   }

   private static <R> SimplePlainQueue<R> queueToUse(boolean infiniteSize, int batchSize) {
      // When tracking keys it is possible that a key can map to more than 1 value - in that case we have to return
      // all values for the same key (which means we could overflow the batchSize queue buffer). Due to this we have
      // to use a queue that will expand beyond the initial size if needed (e.g. SpscLinkedArrayQueue)
      return infiniteSize ? new SpscLinkedArrayQueue<>(batchSize) : new SpscArrayQueue<>(batchSize);
   }

   @Override
   public void subscribe(Subscriber<? super R> s) {
      if (subscriber.compareAndSet(null, s)) {
         if (trace) {
            log.tracef("Subscribed to %s via %s", parent.requestId, s);
         }
         s.onSubscribe(this);
      } else {
         EmptySubscription.error(new IllegalStateException("This processor allows only a single Subscriber"), s);
      }
   }

   @Override
   public void request(long n) {
      if (SubscriptionHelper.validate(n)) {
         BackpressureHelper.add(this, n);
         // Only if there are no other requestors can we proceed
         if (requestors.getAndIncrement() == 0) {
            sendRequest(get());
         }
      }
   }

   @Override
   public void cancel() {
      cancelled = true;
      if (alreadyCreated) {
         Map.Entry<Address, IntSet> target = currentTarget;
         if (target != null) {
            parent.sendCancelCommand(target.getKey());
         }
      }
   }

   /**
    * Method to handle a request of values. This is the only method that should actually publish anything to the
    * subscriber. The requestors variable should also ensure that only one caller may invoke this thread (this
    * guarantee must be non reentrant as well)
    * @param remaining the last known amount of requested values (this value is assumed to always be less than or equal
    *                  to the current request count and greater than 0)
    */
   @GuardedBy("requestors")
   private void sendRequest(long remaining) {
      assert remaining > 0;

      // We first make sure we empty as many entries from the queue up to the remaining amount first before requesting
      // more values
      while (!queue.isEmpty()) {
         long produced = 0;
         // Avoid volatile read for every entry
         Subscriber<? super R> localSubscriber = subscriber.get();
         while (produced < remaining) {
            // Use any of the queued values if present first
            R queuedValue = queue.poll();
            if (queuedValue != null) {
               localSubscriber.onNext(queuedValue);
               produced++;
            } else {
               break;
            }
         }

         remaining = BackpressureHelper.produced(this, produced);

         // We produced some entries from the overflow queue and had leftovers or exactly enough - try to release
         // control of the pendingRequest boolean
         if (remaining == 0) {
            remaining = continueWithRemaining(0);
            if (remaining == 0) {
               // Terminate - continueWithRemaining freed requestors
               return;
            }
            // Means we had a request come when trying to free requestors, so we must continue processing
         }
      }

      assert remaining > 0;

      if (checkCancelled()) {
         return;
      }

      // Find which address and segments we still need to retrieve - when the supplier returns null that means
      // we don't need to do anything else (normal termination state)
      Map.Entry<Address, IntSet> target = currentTarget;
      if (target == null) {
         alreadyCreated = false;
         target = supplier.get();
         if (target == null) {
            if (trace) {
               log.tracef("Completing subscription %s", this);
            }
            subscriber.get().onComplete();
            return;
         } else {
            currentTarget = target;
         }
      }

      Address address = target.getKey();
      IntSet segments = target.getValue();

      CompletionStage<PublisherResponse> stage;
      if (alreadyCreated) {
         stage = parent.sendNextCommand(address, topologyId);
      } else {
         alreadyCreated = true;
         stage = parent.sendInitialCommand(address, segments, batchSize, excludedKeys.remove(address), topologyId);
      }
      stage.whenComplete((values, t) -> {
         if (t != null) {
            handleThrowableInResponse(t, address, segments);
            return;
         }
         try {

            if (trace) {
               // Note the size of the array may not be the amount of entries as it isn't resized (can contain nulls)
               log.tracef("Received %s for id %s from %s", values, parent.requestId, address);
            }

            IntSet completedSegments = values.getCompletedSegments();
            if (completedSegments != null) {
               if (trace) {
                  log.tracef("Completed segments %s for id %s from %s", completedSegments, parent.requestId, address);
               }
               completedSegments.forEach((IntConsumer) parent::completeSegment);
               completedSegments.forEach((IntConsumer) segments::remove);
            }

            IntSet lostSegments = values.getLostSegments();
            if (lostSegments != null) {
               if (trace) {
                  log.tracef("Lost segments %s for id %s from %s", completedSegments, parent.requestId, address);
               }
               lostSegments.forEach((IntConsumer) segments::remove);
            }

            boolean complete = values.isComplete();
            if (complete) {
               // Current address has returned all values it can - setting to null will force the next invocation
               // of this method try the next target if available
               currentTarget = null;
            } else {
               int segment = segments.iterator().nextInt();
               values.forEachSegmentValue(parent, segment);
            }

            long requested = get();
            assert requested > 0;

            long produced = 0;

            Object lastValue = null;

            // Avoid volatile read for every entry
            Subscriber<? super R> localSubscriber = subscriber.get();

            R[] valueArray = (R[]) values.getResults();

            if (values instanceof KeyPublisherResponse) {
               KeyPublisherResponse kpr = (KeyPublisherResponse) values;
               int extraSize = kpr.getExtraSize();
               if (extraSize > 0) {
                  int arrayLength = valueArray.length;
                  Object[] newArray = new Object[arrayLength + extraSize];
                  System.arraycopy(valueArray, 0, newArray, 0, arrayLength);
                  System.arraycopy(kpr.getExtraObjects(), 0, newArray, arrayLength, extraSize);

                  valueArray = (R[]) newArray;
               }
            }

            for (R value : valueArray) {
               if (value == null) {
                  // Local execution doesn't trim array down
                  break;
               }
               if (checkCancelled()) {
                  return;
               }

               // Note that consumed is always equal to how many have been sent to onNext - thus
               // once it is equal to the requested we have to enqueue any additional values - so they can be requested
               // later
               if (produced >= requested) {
                  if (!queue.offer(value)) {
                     throw new MissingBackpressureException("Inner queue full?!");
                  }
               } else {
                  localSubscriber.onNext(value);
                  produced++;
               }
               lastValue = value;
            }

            if (completedSegments != null) {
               // We tell the parent of what the value is when we complete segments - this way they can notify
               // segment listeners properly
               parent.notifySegmentsComplete(completedSegments, lastValue);
            }

            trySendRequest(produced);
         } catch (Throwable innerT) {
            handleThrowableInResponse(innerT, address, segments);
         }
      });
   }

   // This method returns whether this subscription has been cancelled
   // This method doesn't have to be protected by requestors, but there is no reason for a method who doesn't have
   // the requestors "lock" to invoke this
   private boolean checkCancelled() {
      if (cancelled) {
         if (trace) {
            log.tracef("Subscription %s was cancelled, terminating early", this);
         }
         return true;
      }
      return false;
   }

   // If this method is invoked the current thread must not continuing trying to do any additional processing
   @GuardedBy("requestors")
   private void handleThrowableInResponse(Throwable t, Address address, IntSet segments) {
      if (parent.handleThrowable(t, address, segments)) {
         // We were told to continue processing - so ignore those segments and try the next target if possible
         // Since we never invoked parent.compleSegment they may get retried
         currentTarget = null;
         trySendRequest(0);
      }
   }

   /**
    * This method is used by an invoker when it already owns the requestor and will either release it or continue
    * processing more values if not enough have been retrieved or if more requests have come in
    */
   @GuardedBy("requestors")
   private void trySendRequest(long produced) {
      long innerRemaining = continueWithRemaining(produced);
      if (innerRemaining > 0) {
         sendRequest(innerRemaining);
      }
   }

   /**
    * Determines if the current thread should continue processing due to outstanding requests - That means we either
    * need to read from the queue or if empty submit remotely for more elements
    * @param produced how many entries have been produced by the caller
    * @return how many more entries can still be produced or 0 if the caller should immediately return
    */
   @GuardedBy("requestors")
   private long continueWithRemaining(long produced) {
      long remaining = BackpressureHelper.produced(this, produced);
      if (remaining > 0) {
         return remaining;
      }
      do {
         // We try to unregister ourself from being a requestor
         if (requestors.decrementAndGet() == 0) {
            return 0;
         }
         // However if there was another requestor, we now have to check if they updating the remaining count
         remaining = get();
      } while (remaining == 0);

      return remaining;
   }

   @Override
   public String toString() {
      return "InnerPublisher-" + System.identityHashCode(this) +
            "{requestId=" + parent.requestId +
            ", topologyId=" + topologyId +
            '}';
   }
}
