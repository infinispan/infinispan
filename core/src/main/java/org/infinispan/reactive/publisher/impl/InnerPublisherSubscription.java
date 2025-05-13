package org.infinispan.reactive.publisher.impl;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import org.infinispan.commons.reactive.RxJavaInterop;
import org.infinispan.commons.util.IntSet;
import org.infinispan.reactive.publisher.impl.commands.batch.KeyPublisherResponse;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.commons.util.concurrent.CompletableFutures;
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
 *
 * @param <R>
 */
public class InnerPublisherSubscription<K, I, R, E> implements LongConsumer, Action {
   protected static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final InnerPublisherSubscriptionBuilder<K, I, R> builder;
   private final FlowableProcessor<E> flowableProcessor;

   private final AtomicLong requestedAmount = new AtomicLong();

   // The current address and segments we are processing or null if another one should be acquired
   private volatile Map.Entry<Address, IntSet> currentTarget;
   // whether this subscription was cancelled by a caller (means we can stop processing)
   private volatile boolean cancelled;
   // whether the initial request was already sent or not (if so then a next command is used)
   private volatile boolean alreadyCreated;

   private InnerPublisherSubscription(InnerPublisherSubscriptionBuilder<K, I, R> builder,
         FlowableProcessor<E> flowableProcessor, Map.Entry<Address, IntSet> firstTarget) {
      this.builder = builder;
      this.flowableProcessor = flowableProcessor;

      this.currentTarget = firstTarget;
   }

   public static class InnerPublisherSubscriptionBuilder<K, I, R> {
      private final ClusterPublisherManagerImpl<K, ?>.SubscriberHandler<I, R> parent;
      private final int batchSize;
      private final Supplier<Map.Entry<Address, IntSet>> supplier;
      private final Map<Address, Set<K>> excludedKeys;
      private final int topologyId;

      public InnerPublisherSubscriptionBuilder(ClusterPublisherManagerImpl<K, ?>.SubscriberHandler<I, R> parent,
            int batchSize, Supplier<Map.Entry<Address, IntSet>> supplier, Map<Address, Set<K>> excludedKeys,
            int topologyId) {
         this.parent = parent;
         this.batchSize = batchSize;
         this.supplier = supplier;
         this.excludedKeys = excludedKeys;
         this.topologyId = topologyId;
      }

      Publisher<R> createValuePublisher(Map.Entry<Address, IntSet> firstTarget) {
         FlowableProcessor<R> unicastProcessor = UnicastProcessor.create(batchSize);
         InnerPublisherSubscription<K, I, R, R> innerPublisherSubscription = new InnerPublisherSubscription<K, I, R, R>(this,
               unicastProcessor, firstTarget) {
            @Override
            protected void doOnValue(R value, int segment) {
               unicastProcessor.onNext(value);
            }
         };
         return unicastProcessor.doOnLifecycle(RxJavaInterop.emptyConsumer(), innerPublisherSubscription,
               innerPublisherSubscription);
      }

      Publisher<SegmentPublisherSupplier.Notification<R>> createNotificationPublisher(
            Map.Entry<Address, IntSet> firstTarget) {
         FlowableProcessor<SegmentPublisherSupplier.Notification<R>> unicastProcessor = UnicastProcessor.create(batchSize);
         InnerPublisherSubscription<K, I, R, SegmentPublisherSupplier.Notification<R>> innerPublisherSubscription =
               new InnerPublisherSubscription<K, I, R, SegmentPublisherSupplier.Notification<R>>(this, unicastProcessor, firstTarget) {
                  @Override
                  protected void doOnValue(R value, int segment) {
                     unicastProcessor.onNext(Notifications.value(value, segment));
                  }

                  @Override
                  protected void doOnSegmentComplete(int segment) {
                     unicastProcessor.onNext(Notifications.segmentComplete(segment));
                  }
               };
         return unicastProcessor.doOnLifecycle(RxJavaInterop.emptyConsumer(), innerPublisherSubscription,
               innerPublisherSubscription);
      }
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
            builder.parent.sendCancelCommand(target.getKey());
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
            target = builder.supplier.get();
            if (target == null) {
               if (log.isTraceEnabled()) {
                  log.tracef("Completing processor %s", flowableProcessor);
               }
               flowableProcessor.onComplete();
               return;
            } else {
               currentTarget = target;
            }
         }

         ClusterPublisherManagerImpl<K, ?>.SubscriberHandler<I, R> parent = builder.parent;
         CompletionStage<PublisherResponse> stage;
         Address address = target.getKey();
         IntSet segments = target.getValue();
         try {
            if (alreadyCreated) {
               stage = parent.sendNextCommand(address, builder.topologyId);
            } else {
               alreadyCreated = true;
               stage = parent.sendInitialCommand(address, segments, builder.batchSize, builder.excludedKeys.remove(address), builder.topologyId);
            }
         } catch (Throwable t) {
            handleThrowableInResponse(t, address, segments);
            return;
         }

         stage.whenComplete((values, t) -> {
            if (t != null) {
               handleThrowableInResponse(CompletableFutures.extractException(t), address, segments);
               return;
            }
            try {
               if (log.isTraceEnabled()) {
                  // Note the size of the array may not be the amount of entries as it isn't resized (can contain nulls)
                  log.tracef("Received %s for id %s from %s", values, parent.requestId, address);
               }

               IntSet completedSegments = values.getCompletedSegments();
               if (completedSegments != null) {
                  if (log.isTraceEnabled()) {
                     log.tracef("Completed segments %s for id %s from %s", completedSegments, parent.requestId, address);
                  }
                  completedSegments.forEach((IntConsumer) parent::completeSegment);
                  completedSegments.forEach((IntConsumer) segments::remove);
               }

               IntSet lostSegments = values.getLostSegments();
               if (lostSegments != null) {
                  if (log.isTraceEnabled()) {
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
                  values.keysForNonCompletedSegments(parent);
               }

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

               int pos = 0;
               for (PublisherHandler.SegmentResult segmentResult : values.getSegmentResults()) {
                  if (checkCancelled()) {
                     return;
                  }
                  int segment = segmentResult.getSegment();
                  for (int i = 0; i < segmentResult.getEntryCount(); ++i) {
                     R value = valueArray[pos++];
                     doOnValue(value, segment);
                  }
                  if (completedSegments != null && completedSegments.remove(segment)) {
                     doOnSegmentComplete(segment);
                  }
               }

               // Any completed segments left were empty, just complete them together
               if (completedSegments != null) {
                  completedSegments.forEach((IntConsumer) this::doOnSegmentComplete);
               }

               accept(-pos);
            } catch (Throwable innerT) {
               handleThrowableInResponse(innerT, address, segments);
            }
         });
      }
   }

   /**
    * Method invoked on each value providing the value and segment. This method is designed to be overridden by an
    * extended class.
    *
    * @param value published value
    * @param segment segment of the value
    */
   protected void doOnValue(R value, int segment) {

   }

   /**
    * Method invoked whenever a segment is completed. This method is designed to be overridden by an extended class.
    *
    * @param segment completed segment
    */
   protected void doOnSegmentComplete(int segment) {

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
      if (cancelled) {
         // If we were cancelled just log the exception - it may not be an actual problem
         log.tracef("Encountered exception after subscription was cancelled, this can most likely ignored, message is %s", t.getMessage());
      } else if (builder.parent.handleThrowable(t, address, segments)) {
         // We were told to continue processing - so ignore those segments and try the next target if possible
         // Since we never invoked parent.completeSegment they may get retried
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
         if (log.isTraceEnabled()) {
            log.tracef("Subscription %s was cancelled, terminating early", this);
         }
         return true;
      }
      return false;
   }
}
