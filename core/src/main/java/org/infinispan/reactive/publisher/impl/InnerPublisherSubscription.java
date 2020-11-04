package org.infinispan.reactive.publisher.impl;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.reactive.publisher.impl.commands.batch.KeyPublisherResponse;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;

import io.reactivex.rxjava3.functions.Action;
import io.reactivex.rxjava3.functions.LongConsumer;
import io.reactivex.rxjava3.processors.FlowableProcessor;
import io.reactivex.rxjava3.processors.UnicastProcessor;

/**
 * Handles the submission and response handling of an arbitrary amount of address
 * segments. This class will based upon upstream requests send a request to the target address until has retrieved
 * enough entries to satisfy the request threshold. When a given address can no longer return any entries this
 * subscription will try to process the next address/segment combination until it can no longer find any more
 * address/segment targets.
 * <p>
 * Note that this publisher returned via {@link #createPublisher(ClusterPublisherManagerImpl.SubscriberHandler, int, Supplier, Map, int)}
 * can only be subscribed to by one subscriber (more than 1 subscriber will cause issues).
 * @param <R>
 */
public class InnerPublisherSubscription<K, I, R> implements LongConsumer, Action {
   protected final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   protected final boolean trace = log.isTraceEnabled();

   private final ClusterPublisherManagerImpl<K, ?>.SubscriberHandler<I, R> parent;
   private final int batchSize;
   private final Supplier<Map.Entry<Address, IntSet>> supplier;
   private final Map<Address, Set<K>> excludedKeys;
   private final int topologyId;
   private final FlowableProcessor<R> flowableProcessor;

   private final AtomicLong requestedAmount = new AtomicLong();

   // The current address and segments we are processing or null if another one should be acquired
   private volatile Map.Entry<Address, IntSet> currentTarget;
   // whether this subscription was cancelled by a caller (means we can stop processing)
   private volatile boolean cancelled;
   // whether the initial request was already sent or not (if so then a next command is used)
   private volatile boolean alreadyCreated;

   private InnerPublisherSubscription(ClusterPublisherManagerImpl<K, ?>.SubscriberHandler<I, R> parent,
         int batchSize, Supplier<Map.Entry<Address, IntSet>> supplier, Map<Address, Set<K>> excludedKeys, int topologyId,
         FlowableProcessor<R> flowableProcessor, Map.Entry<Address, IntSet> firstTarget) {
      this.parent = parent;
      this.batchSize = batchSize;
      this.supplier = supplier;
      this.excludedKeys = excludedKeys;
      this.topologyId = topologyId;
      this.flowableProcessor = flowableProcessor;

      this.currentTarget = firstTarget;
   }

   static <K, I, R> Publisher<R> createPublisher(ClusterPublisherManagerImpl<K, ?>.SubscriberHandler<I, R> parent,
         int batchSize, Supplier<Map.Entry<Address, IntSet>> supplier, Map<Address, Set<K>> excludedKeys, int topologyId) {
      return createPublisher(parent, batchSize, supplier, excludedKeys, topologyId, null);
   }

   static <K, I, R> Publisher<R> createPublisher(ClusterPublisherManagerImpl<K, ?>.SubscriberHandler<I, R> parent,
         int batchSize, Supplier<Map.Entry<Address, IntSet>> supplier, Map<Address, Set<K>> excludedKeys, int topologyId,
         Map.Entry<Address, IntSet> firstTarget) {
      FlowableProcessor<R> unicastProcessor = UnicastProcessor.create(batchSize);
      InnerPublisherSubscription<K, I, R> innerPublisherSubscription = new InnerPublisherSubscription<>(parent,
            batchSize, supplier, excludedKeys, topologyId, unicastProcessor, firstTarget);
      return unicastProcessor.doOnLifecycle(RxJavaInterop.emptyConsumer(), innerPublisherSubscription,
            innerPublisherSubscription);
   }

   /**
    * This is invoked when the flowable is completed - need to close any pending publishers
    */
   @Override
   public void run() {
      cancelled = true;
      if (alreadyCreated) {
         Map.Entry<Address, IntSet> target = currentTarget;
         if (target != null) {
            parent.sendCancelCommand(target.getKey());
         }
      }
   }

   /**
    * This method is invoked every time a new request is sent to the underlying publisher. We need to submit a request
    * if there is not a pending one. Whenever requestedAmount is a number greater than 0, that means we must submit or
    * there is a pending one.
    * @param count request count
    */
   @Override
   public void accept(long count) {
      if (shouldSubmit(count)) {
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
                  log.tracef("Completing processor %s", flowableProcessor);
               }
               flowableProcessor.onComplete();
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

               long produced = 0;

               Object lastValue = null;

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

                  flowableProcessor.onNext(value);
                  produced++;
                  lastValue = value;
               }

               if (completedSegments != null) {
                  // We tell the parent of what the value is when we complete segments - this way they can notify
                  // segment listeners properly
                  parent.notifySegmentsComplete(completedSegments, lastValue);
               }

               accept(-produced);
            } catch (Throwable innerT) {
               handleThrowableInResponse(innerT, address, segments);
            }
         });
      }
   }

   private boolean shouldSubmit(long count) {
      while (true) {
         long prev = requestedAmount.get();
         long newValue = prev + count;
         if (requestedAmount.compareAndSet(prev, newValue)) {
            // This ensures that only a single submission can be done at one time
            // It will only submit if there were none prior (prev <= 0) or if it is the current one (count <= 0).
            return newValue > 0 && (prev <= 0 || count <= 0);
         }
      }
   }

   // If this method is invoked the current thread must not continuing trying to do any additional processing
   private void handleThrowableInResponse(Throwable t, Address address, IntSet segments) {
      if (parent.handleThrowable(t, address, segments)) {
         // We were told to continue processing - so ignore those segments and try the next target if possible
         // Since we never invoked parent.compleSegment they may get retried
         currentTarget = null;
         // Try to retrieve entries from the next node if possible
         accept(0);
      } else {
         flowableProcessor.onError(t);
      }
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
}
