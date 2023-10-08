package org.infinispan.reactive.publisher.impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.reactive.RxJavaInterop;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.KeyPublisherResponse;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableSubscriber;
import io.reactivex.rxjava3.core.Single;
import net.jcip.annotations.GuardedBy;

/**
 * Handler for holding publisher results between requests of data
 * @since 10.1
 */
@Scope(Scopes.NAMED_CACHE)
@Listener(observation = Listener.Observation.POST)
public class PublisherHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

   private final ConcurrentMap<Object, PublisherState> currentRequests = new ConcurrentHashMap<>();

   @Inject CacheManagerNotifier managerNotifier;
   @Inject @ComponentName(KnownComponentNames.NON_BLOCKING_EXECUTOR)
   ExecutorService nonBlockingExecutor;
   @Inject LocalPublisherManager localPublisherManager;

   @ViewChanged
   public void viewChange(ViewChangedEvent event) {
      List<Address> newMembers = event.getNewMembers();
      Iterator<PublisherState> iter = currentRequests.values().iterator();
      while (iter.hasNext()) {
         PublisherState state = iter.next();

         Address owner = state.getOrigin();
         // If an originating node is no longer here then we have to close their publishers - null means local node
         // so that can't be suspected
         if (owner != null && !newMembers.contains(owner)) {
            log.tracef("View changed and no longer contains %s, closing %s publisher", owner, state.requestId);
            state.cancel();
            iter.remove();
         }
      }
   }

   @Start
   public void start() {
      managerNotifier.addListener(this);
   }

   @Stop
   public void stop() {
      // If our cache is stopped we should remove our listener, since this doesn't mean the cache manager is stopped
      managerNotifier.removeListener(this);
   }

   /**
    * Registers a publisher given the initial command arguments. The value returned will eventually contain the
    * first batched response for the publisher of the given id.
    * @param command the command with arguments to start a publisher with
    * @param <I> input type
    * @param <R> output type
    * @return future that will or eventually will contain the first response
    */
   public <I, R> CompletableFuture<PublisherResponse> register(InitialPublisherCommand<?, I, R> command) {
      PublisherState publisherState;
      String requestId = command.getRequestId();
      if (command.isTrackKeys()) {
         publisherState = new KeyPublisherState(requestId, command.getOrigin(), command.getBatchSize());
      } else {
         publisherState = new PublisherState(requestId, command.getOrigin(), command.getBatchSize());
      }

      PublisherState previousState;
      if ((previousState = currentRequests.put(requestId, publisherState)) != null) {
         if (!previousState.complete) {
            currentRequests.remove(requestId);
            throw new IllegalStateException("There was already a publisher registered for id " + requestId + " that wasn't complete!");
         }
         // We have a previous state that is already completed - this is most likely due to a failover and our node
         // now owns another segment but the async thread hasn't yet cleaned up our state.
         if (log.isTraceEnabled()) {
            log.tracef("Closing prior state for %s to make room for a new request", requestId);
         }
         previousState.cancel();
      }

      publisherState.startProcessing(command);

      return publisherState.results();
   }

   /**
    * Retrieves the next response for the same request id that was configured on the command when invoking
    * {@link #register(InitialPublisherCommand)}.
    * @param requestId the unique request id to continue the response with
    * @return future that will or eventually will contain the next response
    */
   public CompletableFuture<PublisherResponse> getNext(String requestId) {
      PublisherState publisherState = currentRequests.get(requestId);
      if (publisherState == null) {
         throw new IllegalStateException("Publisher for requestId " + requestId + " doesn't exist!");
      }
      return publisherState.results();
   }

   /**
    * Returns how many publishers are currently open
    * @return how many publishers are currently open
    */
   public int openPublishers() {
      return currentRequests.size();
   }

   /**
    * Closes the publisher that maps to the given request id
    * @param requestId unique identifier for the request
    */
   public void closePublisher(String requestId) {
      PublisherState state;
      if ((state = currentRequests.remove(requestId)) != null) {
         if (log.isTraceEnabled()) {
            log.tracef("Closed publisher using requestId %s", requestId);
         }
         state.cancel();
      }
   }

   /**
    * Optionally closes the state if this state is still registered for the given requestId
    * @param requestId unique identifier for the given request
    * @param state state to cancel if it is still registered
    */
   private void closePublisher(String requestId, PublisherState state) {
      if (currentRequests.remove(requestId, state)) {
         if (log.isTraceEnabled()) {
            log.tracef("Closed publisher from completion using requestId %s", requestId);
         }
         state.cancel();
      } else if (log.isTraceEnabled()) {
         log.tracef("A concurrent request already closed the prior state for %s", requestId);
      }
   }

   @ProtoTypeId(ProtoStreamTypeIds.PUBLISHER_HANDLER_SEGMENT_RESPONSE)
   public static class SegmentResult {
      @ProtoField(value = 1, defaultValue = "-1")
      final int segment;

      @ProtoField(value = 2, defaultValue = "-1")
      final int entryCount;

      @ProtoFactory
      public SegmentResult(int segment, int entryCount) {
         this.segment = segment;
         this.entryCount = entryCount;
      }

      public int getEntryCount() {
         return entryCount;
      }

      public int getSegment() {
         return segment;
      }

      @Override
      public String toString() {
         return "SegmentResult{" +
               "segment=" + segment +
               ", entryCount=" + entryCount +
               '}';
      }
   }

   /**
    * Actual subscriber that listens to the local publisher and stores state and prepares responses as they are ready.
    * This subscriber works by initially requesting {@code batchSize + 1} entries when it is subscribed. The {@code +1}
    * is done purposefully due to how segment completion is guaranteed to be notified just before the next value of
    * a different segment is returned. This way a given batchSize will have a complete view of which segments were
    * completed in it. Subsequent requests will only request {@code batchSize} since our outstanding request count
    * is always 1 more.
    * <p>
    * When a batch size is retrieved or the publisher is complete we create a PublisherResponse that is either
    * passed to the waiting CompletableFuture or registers a new CompletableFuture for a pending request to receive.
    * <p>
    * The state keeps track of all segments that have completed or lost during the publisher response and are returned
    * on the next response. The state keeps track of where the last segment completed {@code segmentStart}, thus
    * our response can tell which which values were not part of the completed segments. It also allows us to drop entries
    * from a segment that was just lost. This is preferable since otherwise the coordinator will have to resend this
    * value or retrieve the value a second time, thus reducing how often they keys need to be replicated.
    * <p>
    * This class relies heavily upon the fact that the reactive stream spec specifies that {@code onNext},
    * {@code onError}, and {@code onComplete} are invoked in a thread safe manner as well as the {@code accept} method
    * on the {@code IntConsumer} when a segment is completed or lost. This allows us to use a simple array with an offset
    * that is used to collect the response.
    */
   private class PublisherState implements FlowableSubscriber<SegmentAwarePublisherSupplier.NotificationWithLost<Object>>, Runnable {
      final String requestId;
      final Address origin;
      final int batchSize;

      // Stores future responses - Normally this only ever contains zero or one result. This can contain two in the
      // case of having a single entry in the last result. Due to the nature of having to request one additional
      // entry to see segment completion, this is the tradeoff
      @GuardedBy("this")
      private CompletableFuture<PublisherResponse> futureResponse = null;

      Subscription upstream;

      // The remainder of the values hold the values between results received - These do not need synchronization
      // as the Subscriber contract guarantees these are invoked serially and has proper visibility
      Object[] results;
      List<SegmentResult> segmentResults;
      int pos;
      IntSet completedSegments;
      IntSet lostSegments;
      int currentSegment = -1;
      int segmentEntries;
      // Set to true when the last futureResponse has been set - meaning the next response will be the last
      volatile boolean complete;

      private PublisherState(String requestId, Address origin, int batchSize) {
         this.requestId = requestId;
         this.origin = origin;
         this.batchSize = batchSize;

         results = new Object[batchSize];
      }

      void startProcessing(InitialPublisherCommand command) {
         SegmentAwarePublisherSupplier<Object> sap;
         if (command.isEntryStream()) {
            sap = localPublisherManager.entryPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                                                       command.getExplicitFlags(), command.getDeliveryGuarantee(), command.getTransformer());
         } else {
            sap = localPublisherManager.keyPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                                                     command.getExplicitFlags(), command.getDeliveryGuarantee(), command.getTransformer());
         }

         Flowable.fromPublisher(sap.publisherWithLostSegments(true))
               .subscribe(this);
      }

      @Override
      public void onSubscribe(Subscription s) {
         if (upstream != null) {
            throw new IllegalStateException("Subscription was already set!");
         }
         this.upstream = Objects.requireNonNull(s);
         requestMore(s, batchSize);
      }

      protected void requestMore(Subscription subscription, int requestAmount) {
         subscription.request(requestAmount);
      }

      @Override
      public void onError(Throwable t) {
         complete = true;
         log.trace("Exception encountered while processing publisher", t);
         synchronized (this) {
            if (futureResponse == null) {
               futureResponse = CompletableFuture.failedFuture(t);
            } else {
               futureResponse.completeExceptionally(t);
            }
         }
      }

      @Override
      public void onComplete() {
         prepareResponse(true);
         if (log.isTraceEnabled()) {
            log.tracef("Completed state for %s", requestId);
         }
      }

      @Override
      public void onNext(SegmentAwarePublisherSupplier.NotificationWithLost notification) {
         if (!notification.isValue()) {
            int segment;
            if (notification.isSegmentComplete()) {
               segment = notification.completedSegment();
               if (segmentEntries > 0) {
                  addToSegmentResults(segment, segmentEntries);
               }
               segmentComplete(segment);
            } else {
               segment = notification.lostSegment();
               segmentLost(segment);
            }

            // Need to request more data as our responses are based on entries and not segments
            requestMore(upstream, 1);
            return;
         }
         int segment = notification.valueSegment();

         assert currentSegment == segment || currentSegment == -1;
         currentSegment = segment;
         segmentEntries++;

         results[pos++] = notification.value();
         // Means we just finished a batch
         if (pos == results.length) {
            prepareResponse(false);
         }
      }

      public void segmentComplete(int segment) {
         assert currentSegment == segment || currentSegment == -1;

         if (log.isTraceEnabled()) {
            log.tracef("Completing segment %s for %s", segment, requestId);
         }

         if (completedSegments == null) {
            completedSegments = IntSets.mutableEmptySet();
         }
         completedSegments.set(segment);

         segmentEntries = 0;
         currentSegment = -1;
      }

      public void segmentLost(int segment) {
         assert currentSegment == segment || currentSegment == -1;

         if (log.isTraceEnabled()) {
            log.tracef("Lost segment %s for %s", segment, requestId);
         }

         if (lostSegments == null) {
            lostSegments = IntSets.mutableEmptySet();
         }
         lostSegments.set(segment);

         // Just reset the pos back to the segment start - ignoring those entries
         // This saves us from sending these entries back and then having to resend the key to the new owner
         pos -= segmentEntries;
         segmentEntries = 0;
         currentSegment = -1;
      }

      public void cancel() {
         Subscription subscription = upstream;
         if (subscription != null) {
            subscription.cancel();
         }
      }

      void resetValues() {
         this.results = new Object[batchSize];
         this.segmentResults = null;
         this.completedSegments = null;
         this.lostSegments = null;
         this.pos = 0;
         this.currentSegment = -1;
         this.segmentEntries = 0;
      }

      PublisherResponse generateResponse(boolean complete) {
         return new PublisherResponse(results, completedSegments, lostSegments, pos, complete,
               segmentResults == null ? Collections.emptyList() : segmentResults);
      }

      void prepareResponse(boolean complete) {
         if (currentSegment != -1) {
            addToSegmentResults(currentSegment, segmentEntries);
         }

         PublisherResponse response = generateResponse(complete);

         if (log.isTraceEnabled()) {
            log.tracef("Response ready %s with id %s for requestor %s", response, requestId, origin);
         }

         if (!complete) {
            // Have to reset the values if we expect to send another response
            resetValues();
         }

         this.complete = complete;

         CompletableFuture<PublisherResponse> futureToComplete = null;
         synchronized (this) {
            if (futureResponse != null) {
               if (futureResponse.isDone()) {
                  if (!futureResponse.isCompletedExceptionally()) {
                     throw new IllegalStateException("Response already completed with " + CompletionStages.join(futureResponse) +
                           " but we want to complete with " + response);
                  }
                  log.tracef("Response %s already completed with an exception, ignoring values", System.identityHashCode(futureResponse));
               }
               futureToComplete = futureResponse;
               futureResponse = null;
            } else {
               futureResponse = CompletableFuture.completedFuture(response);
               if (log.isTraceEnabled()) {
                  log.tracef("Eager response completed %d for request id %s", System.identityHashCode(futureResponse), requestId);
               }
            }
         }
         if (futureToComplete != null) {
            if (log.isTraceEnabled()) {
               log.tracef("Completing waiting future %d for request id %s", System.identityHashCode(futureToComplete), requestId);
            }
            // Complete this outside of synchronized block
            futureToComplete.complete(response);
         }
      }

      public Address getOrigin() {
         return origin;
      }

      /**
       * Retrieves the either already completed result or registers a new future to be completed. This also prestarts
       * the next batch to be ready for the next request as it comes, which is submitted on the {@link #nonBlockingExecutor}.
       * @return future that will contain the publisher response with the data
       */
      CompletableFuture<PublisherResponse> results() {
         boolean submitRequest = false;
         CompletableFuture<PublisherResponse> currentFuture;
         synchronized (this) {
            if (futureResponse == null) {
               currentFuture = new CompletableFuture<>();
               currentFuture.thenRunAsync(this, nonBlockingExecutor);
               futureResponse = currentFuture;
            } else {
               currentFuture = futureResponse;
               futureResponse = null;
               submitRequest = true;
            }
         }
         if (submitRequest) {
            // Handles closing publisher or requests next batch if not complete
            // Note this is not done in synchronized block in case if executor is within thread
            nonBlockingExecutor.execute(this);
         }
         if (log.isTraceEnabled()) {
            log.tracef("Retrieved future %d for request id %s", System.identityHashCode(currentFuture), requestId);
         }
         return currentFuture;
      }

      void addToSegmentResults(int segment, int entryCount) {
         if (segmentResults == null) {
            segmentResults = new ArrayList<>();
         }
         segmentResults.add(new SegmentResult(segment, entryCount));
      }

      /**
       * This will either request the next batch of values or completes the request. Note the completion has to be done
       * after the last result is returned, thus it cannot be eagerly closed in most cases.
       */
      @Override
      public void run() {
         if (log.isTraceEnabled()) {
            log.tracef("Running handler for request id %s", requestId);
         }
         if (!complete) {
            int requestAmount = batchSize;
            if (log.isTraceEnabled()) {
               log.tracef("Requesting %d additional entries for %s", requestAmount, requestId);
            }
            requestMore(upstream, requestAmount);
         } else {
            synchronized (this) {
               if (futureResponse == null) {
                  closePublisher(requestId, this);
               } else if (log.isTraceEnabled()) {
                  log.tracef("Skipping run as handler is complete, but still has some results for id %s", requestId);
               }
            }
         }
      }
   }

   /**
    * Special PublisherState that listens also to what key generates a given set of values. This state is only used
    * when keys must be tracked (EXACTLY_ONCE guarantee with map or flatMap)
    * <p>
    * The general idea is the publisher will notify when a key or entry (referred to as just key from now on) is sent
    * down the pipeline and we can view all values that result from that. Thus we only send a result when we have enough
    * values (>= batchSize) but also get to a new key. This means that we can
    * actually return more values than the batchSize when flatMap returns more than 1 value for a given key.
    */
   class KeyPublisherState extends PublisherState {
      Object[] extraValues;
      int extraPos;
      Object[] keys;
      int keyPos;

      int keyStartPosition;

      private KeyPublisherState(String requestId, Address origin, int batchSize) {
         super(requestId, origin, batchSize);
      }

      // Class to signal down that a given key was completed for key tracking purposes
      class KeyCompleted<E> extends Notifications.ReuseNotificationBuilder<E> {
         @Override
         public String toString() {
            return "KeyCompleted{" +
                  "key=" + value +
                  ", segment=" + segment +
                  '}';
         }
      }

      @Override
      void startProcessing(InitialPublisherCommand command) {
         SegmentAwarePublisherSupplier<Object> sap;
         io.reactivex.rxjava3.functions.Function<Object, Object> toKeyFunction;
         if (command.isEntryStream()) {
            sap = localPublisherManager.entryPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                                                       command.getExplicitFlags(), DeliveryGuarantee.EXACTLY_ONCE, Function.identity());
            toKeyFunction = (io.reactivex.rxjava3.functions.Function) RxJavaInterop.entryToKeyFunction();
         } else {
            sap = localPublisherManager.keyPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                                                     command.getExplicitFlags(), DeliveryGuarantee.EXACTLY_ONCE, Function.identity());
            toKeyFunction = RxJavaInterop.identityFunction();
         }

         Function<Publisher<Object>, Publisher<Object>> functionToApply = command.getTransformer();

         // We immediately consume the value, so we can reuse a builder for both to save on allocations
         Notifications.NotificationBuilder<Object> builder = Notifications.reuseBuilder();
         KeyCompleted<Object> keyBuilder = new KeyCompleted<>();

         Flowable.fromPublisher(sap.publisherWithLostSegments())
               .concatMap(notification -> {
                  if (!notification.isValue()) {
                     return Flowable.just(notification);
                  }
                  Object originalValue = notification.value();
                  Object key = toKeyFunction.apply(originalValue);
                  return Flowable.fromPublisher(functionToApply.apply(Flowable.just(originalValue)))
                        .map(v -> builder.value(v, notification.valueSegment()))
                        // Signal the end of the key - flatMap could have 0 or multiple entries
                        .concatWith(Single.just(keyBuilder.value(key, notification.valueSegment())));
               })
               .subscribe(this);
      }

      @Override
      PublisherResponse generateResponse(boolean complete) {
         return new KeyPublisherResponse(results, completedSegments, lostSegments, pos, complete,
               segmentResults == null ? Collections.emptyList() : segmentResults, extraValues, extraPos, keys, keyPos);
      }

      @Override
      public void onNext(SegmentAwarePublisherSupplier.NotificationWithLost notification) {
         if (!notification.isValue()) {
            super.onNext(notification);
            return;
         }
         boolean requestMore = true;
         if (notification instanceof KeyCompleted) {
            // If these don't equal that means the key had some values mapped to it, so we need to retain the key
            // in case if we can't finish this segment and user needs to retry
            if (keyStartPosition != pos) {
               Object key = notification.value();
               if (keys == null) {
                  // This is the largest the array can be
                  keys = new Object[batchSize];
               }
               keys[keyPos++] = key;

               if (pos == results.length) {
                  prepareResponse(false);
                  // We don't request more if we completed a batch - we will request later after the result is returned
                  requestMore = false;
               } else {
                  keyStartPosition = pos;
               }
            }

            if (requestMore) {
               requestMore(upstream, 1);
            }

            return;
         }

         int segment = notification.valueSegment();
         assert currentSegment == segment || currentSegment == -1;
         currentSegment = segment;
         segmentEntries++;

         Object value = notification.value();
         if (pos == results.length) {
            // Write any overflow into our buffer
            if (extraValues == null) {
               extraValues = new Object[8];
            }
            if (extraPos == extraValues.length) {
               Object[] expandedArray = new Object[extraValues.length << 1];
               System.arraycopy(extraValues, 0, expandedArray, 0, extraPos);
               extraValues = expandedArray;
            }
            extraValues[extraPos++] = value;

            // Need to keep requesting until we get to the end of the key
            requestMore(upstream, 1);
         } else {
            results[pos++] = value;

            // If we have filled up the array, we need to request until we hit end of key
            if (pos == results.length) {
               requestMore(upstream, 1);
            }
         }
      }

      @Override
      void resetValues() {
         super.resetValues();
         keyResetValues();
      }

      void keyResetValues() {
         extraValues = null;
         extraPos = 0;
         keys = null;
         keyPos = 0;
         keyStartPosition = 0;
      }

      @Override
      public void segmentComplete(int segment) {
         super.segmentComplete(segment);
         keys = null;
         keyPos = 0;
         keyStartPosition = 0;
      }

      @Override
      public void segmentLost(int segment) {
         super.segmentLost(segment);
         // We discard any extra values as they would all be in the same segment - the super method already discarded
         // the non extra values
         keyResetValues();
      }
   }
}
