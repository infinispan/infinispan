package org.infinispan.reactive.publisher.impl;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.infinispan.commons.util.ByRef;
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
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.KeyPublisherResponse;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.FlowableSubscriber;
import net.jcip.annotations.GuardedBy;

/**
 * Handler for holding publisher results between requests of data
 * @since 10.1
 */
@Scope(Scopes.NAMED_CACHE)
@Listener(observation = Listener.Observation.POST)
public class PublisherHandler {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private final boolean trace = log.isTraceEnabled();

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
         if (trace) {
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
         if (trace) {
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
         if (trace) {
            log.tracef("Closed publisher from completion using requestId %s", requestId);
         }
         state.cancel();
      } else if (trace) {
         log.tracef("A concurrent request already closed the prior state for %s", requestId);
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
   private class PublisherState implements FlowableSubscriber<Object>, Runnable {
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
      int pos;
      IntSet completedSegments;
      IntSet lostSegments;
      int segmentStart;
      // Set to true when the last futureResponse has been set - meaning the next response will be the last
      volatile boolean complete;

      private PublisherState(String requestId, Address origin, int batchSize) {
         this.requestId = requestId;
         this.origin = origin;
         this.batchSize = batchSize;

         results = new Object[batchSize];
      }

      void startProcessing(InitialPublisherCommand command) {
         SegmentAwarePublisher sap;
         if (command.isEntryStream()) {
            sap = localPublisherManager.entryPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                  command.isIncludeLoader(), command.getDeliveryGuarantee(), command.getTransformer());
         } else {
            sap = localPublisherManager.keyPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                  command.isIncludeLoader(), command.getDeliveryGuarantee(), command.getTransformer());
         }

         sap.subscribe(this, this::segmentComplete, this::segmentLost);
      }

      @Override
      public void onSubscribe(Subscription s) {
         if (upstream != null) {
            throw new IllegalStateException("Subscription was already set!");
         }
         this.upstream = Objects.requireNonNull(s);
         // We request 1 extra to guarantee we see the segment complete/lost message
         requestMore(s, batchSize + 1);
      }

      protected void requestMore(Subscription subscription, int requestAmount) {
         subscription.request(requestAmount);
      }

      @Override
      public void onError(Throwable t) {
         complete = true;
         synchronized (this) {
            futureResponse = CompletableFutures.completedExceptionFuture(t);
         }
      }

      @Override
      public void onComplete() {
         prepareResponse(true);
      }

      @Override
      public void onNext(Object o) {
         // Means we just finished a batch
         if (pos == results.length) {
            prepareResponse(false);
         }
         results[pos++] = o;
      }

      public void segmentComplete(int segment) {
         if (completedSegments == null) {
            completedSegments = IntSets.mutableEmptySet();
         }
         completedSegments.add(segment);
         segmentStart = pos;
      }

      public void segmentLost(int segment) {
         if (lostSegments == null) {
            lostSegments = IntSets.mutableEmptySet();
         }
         lostSegments.add(segment);
         // Just reset the pos back to the segment start - ignoring those entries
         // This saves us from sending these entries back and then having to resend the key to the new owner
         pos = segmentStart;
      }

      public void cancel() {
         Subscription subscription = upstream;
         if (subscription != null) {
            subscription.cancel();
         }
      }

      void resetValues() {
         this.results = new Object[batchSize];
         this.completedSegments = null;
         this.lostSegments = null;
         this.pos = 0;
         this.segmentStart = 0;
      }

      PublisherResponse generateResponse(boolean complete) {
         return new PublisherResponse(results, completedSegments, lostSegments, pos, complete, segmentStart);
      }

      void prepareResponse(boolean complete) {
         PublisherResponse response = generateResponse(complete);

         if (trace) {
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
                  // If future was done, that means we prefetched the response - so we may as well merge the results
                  // together (this happens if last entry was by itself - so we will return batchSize + 1)
                  PublisherResponse prevResponse = futureResponse.join();
                  PublisherResponse newResponse = mergeResponses(prevResponse, response);
                  futureResponse = CompletableFuture.completedFuture(newResponse);
                  if (trace) {
                     log.tracef("Received additional response, merged responses together %d for request id %s", System.identityHashCode(futureResponse), requestId);
                  }
               } else {
                  futureToComplete = futureResponse;
                  futureResponse = null;
               }
            } else {
               futureResponse = CompletableFuture.completedFuture(response);
               if (trace) {
                  log.tracef("Eager response completed %d for request id %s", System.identityHashCode(futureResponse), requestId);
               }
            }
         }
         if (futureToComplete != null) {
            if (trace) {
               log.tracef("Completing waiting future %d for request id %s", System.identityHashCode(futureToComplete), requestId);
            }
            // Complete this outside of synchronized block
            futureToComplete.complete(response);
         }
      }

      PublisherResponse mergeResponses(PublisherResponse response1, PublisherResponse response2) {
         IntSet completedSegments = mergeSegments(response1.getCompletedSegments(), response2.getCompletedSegments());
         IntSet lostSegments = mergeSegments(response1.getLostSegments(), response2.getLostSegments());

         int newSize = response1.getSize() + response2.getSize();
         Object[] newArray = new Object[newSize];
         int offset = 0;
         offset = addToArray(response1.getResults(), newArray, offset);
         addToArray(response2.getResults(), newArray, offset);
         // This should always be true
         boolean complete = response2.isComplete();
         assert complete;
         return new PublisherResponse(newArray, completedSegments, lostSegments, newSize, complete, newArray.length);
      }

      int addToArray(Object[] src, Object[] dst, int offset) {
         if (src != null) {
            for (Object obj : src) {
               if (obj == null) {
                  break;
               }
               dst[offset++] = obj;
            }
         }
         return offset;
      }

      IntSet mergeSegments(IntSet segments1, IntSet segments2) {
         if (segments1 == null) {
            return segments2;
         } else if (segments2 == null) {
            return segments1;
         }
         segments1.addAll(segments2);
         return segments1;
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
         if (trace) {
            log.tracef("Retrieved future %d for request id %s", System.identityHashCode(currentFuture), requestId);
         }
         return currentFuture;
      }

      /**
       * This will either request the next batch of values or completes the request. Note the completion has to be done
       * after the last result is returned, thus it cannot be eagerly closed in most cases.
       */
      @Override
      public void run() {
         if (trace) {
            log.tracef("Running handler for request id %s", requestId);
         }
         if (!complete) {
            int requestAmount = batchSize;
            if (trace) {
               log.tracef("Requesting %d additional entries for %s", requestAmount, requestId);
            }
            requestMore(upstream, requestAmount);
         } else {
            synchronized (this) {
               if (futureResponse == null) {
                  closePublisher(requestId, this);
               } else if (trace) {
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

      // How many values we have published (note that this can higher or lower than the consumerOffset). It will be
      // higher when values are buffered in the concatMap operator but it also can be lower when the value is passed
      // directly through to onNext (as we don't increment it until after all values for a given key are consumed).
      long publisherOffset;
      // How many values we have seen in onNext
      long consumerOffset;
      // Keeps track of how many elements we have requested from the upstream - this is needed when doing flatMap
      // because we request batchSize + 1 which may not be enough to complete a key
      long requestOffset;

      // Keeps track of the last key seen so that when a segment is complete we know what the last key is for that segment
      // which is stored in the keySegmentCompletions map
      Object keyForSegmentCompletion;

      // Stores for a given key which segment it will complete when consumed
      Map<Object, Integer> keySegmentCompletions = new HashMap<>();

      // Stores for a given key when the key is consumed (matching the consumer offset
      Map<Long, Object> keyCompletionPosition = new HashMap<>();

      // Whether the last published value completed a key (we cannot send a respones in the middle of processing a key)
      boolean previousValueFinishedKey = true;

      private KeyPublisherState(String requestId, Address origin, int batchSize) {

         super(requestId, origin, batchSize);
         // Worst case is keys found is same size as the batch 1:1
         keys = new Object[batchSize];
      }

      @Override
      void startProcessing(InitialPublisherCommand command) {
         SegmentAwarePublisher<Object> sap;
         if (command.isEntryStream()) {
            sap = localPublisherManager.entryPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                  command.isIncludeLoader(), DeliveryGuarantee.EXACTLY_ONCE, Function.identity());
         } else {
            sap = localPublisherManager.keyPublisher(command.getSegments(), command.getKeys(), command.getExcludedKeys(),
                  command.isIncludeLoader(), DeliveryGuarantee.EXACTLY_ONCE, Function.identity());
         }

         Function<Publisher<Object>, Publisher<Object>> functionToApply = command.getTransformer();

         Flowable.fromPublisher(s -> sap.subscribe(s, this::segmentComplete, this::segmentLost))
               // We need to do this first because it is a PASS_THROUGH operation - so we can maintain segment
               // completion ordering with the assignment of this variable
               .doOnNext(originalValue -> keyForSegmentCompletion = originalValue)
               // This is a FULL backpressure operation that buffers values thus causes values to not immediatelly
               // be published
               .concatMap(originalValue -> {
                  ByRef.Integer size = new ByRef.Integer(0);
                  return Flowable.fromPublisher(functionToApply.apply(Flowable.just(originalValue)))
                        .doOnNext(ignore -> size.inc())
                        .doOnComplete(() -> {
                              int total = size.get();
                              if (total > 0) {
                                 publisherOffset += total;
                                 // Means our values were consumed downstream immediately and thus our key is complete
                                 if (publisherOffset == consumerOffset) {
                                    keyCompleted(originalValue);
                                    previousValueFinishedKey = true;
                                 } else {
                                    keyCompletionPosition.put(publisherOffset, originalValue);
                                 }
                              } else {
                                 // If there are no values for the key it is complete but also doesn't need to
                                 // be tracked, so complete any segment tied to it if possible
                                 Integer segment = keySegmentCompletions.remove(originalValue);
                                 if (segment != null) {
                                    if (trace) {
                                       log.tracef("Completing segment %s due to empty resulting value of %s for %s",
                                             segment, originalValue, requestId);
                                    }
                                    actualCompleteSegment(segment);
                                 }
                                 // Also null out our key for segment completion if needed since this key will never be
                                 // found published
                                 if (keyForSegmentCompletion == originalValue) {
                                    keyForSegmentCompletion = null;
                                 }
                              }
                        });
               })
               .subscribe(this);
      }

      @Override
      PublisherResponse generateResponse(boolean complete) {
         return new KeyPublisherResponse(results, completedSegments, lostSegments, pos, complete, extraValues, extraPos,
               keys, keyPos);
      }

      @Override
      PublisherResponse mergeResponses(PublisherResponse publisherResponse1, PublisherResponse publisherResponse2) {

         KeyPublisherResponse response1 = (KeyPublisherResponse) publisherResponse1;
         KeyPublisherResponse response2 = (KeyPublisherResponse) publisherResponse2;
         IntSet completedSegments = mergeSegments(response1.getCompletedSegments(), response2.getCompletedSegments());
         IntSet lostSegments = mergeSegments(response1.getLostSegments(), response2.getLostSegments());

         int newSize = response1.getSize() + response1.getExtraSize() + response2.getSize() + response2.getExtraSize();
         Object[] newArray = new Object[newSize];
         int offset = 0;

         offset = addToArray(response1.getResults(), newArray, offset);
         offset = addToArray(response1.getExtraObjects(), newArray, offset);
         offset = addToArray(response2.getResults(), newArray, offset);
         addToArray(response2.getExtraObjects(), newArray, offset);

         // This should always be true
         boolean complete = response2.isComplete();
         assert complete;
         return new PublisherResponse(newArray, completedSegments, lostSegments, newSize, complete, newArray.length);
      }

      @Override
      public void onComplete() {
         if (trace) {
            log.tracef("Completed state for %s", requestId);
         }
         super.onComplete();
      }

      @Override
      protected void requestMore(Subscription subscription, int requestAmount) {
         requestOffset += requestAmount;
         super.requestMore(subscription, requestAmount);
      }

      private void keyCompleted(Object key) {
         // This means we processed a key without fetching another - thus we must allow if a segment completion
         // comes next to actually complete
         if (keyForSegmentCompletion == key) {
            keyForSegmentCompletion = null;
         }

         Integer segmentToComplete = keySegmentCompletions.remove(key);
         if (segmentToComplete != null) {
            if (trace) {
               log.tracef("Completing segment %s from key %s for %s", segmentToComplete, key, requestId);
            }
            actualCompleteSegment(segmentToComplete);
            keys = null;
            keyPos = 0;
         } else {
            // We don't need to track the key for a segment that just completed
            if (keys == null) {
               keys = new Object[batchSize];
            }
            keys[keyPos++] = key;
         }
      }

      @Override
      public void onNext(Object value) {
         if (previousValueFinishedKey) {
            // We can only send a response when we found a key completed for the prior value
            // We do this so that the very last entry doesn't send a response and the onComplete will instead
            tryPrepareResponse();
         }

         Object key = keyCompletionPosition.remove(++consumerOffset);
         previousValueFinishedKey = key != null;
         if (previousValueFinishedKey) {
            keyCompleted(key);
         }

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

            // It is possible the overflow has used up the requested amount but a key hasn't completed - we need
            // to request some more values just to be safe
            if (consumerOffset == requestOffset) {
               requestMore(upstream, 2);
            }
         } else {
            results[pos++] = value;
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
      }

      // Technically this is notified after the last value for a given segment - which means if we filled the buffer
      // with the last entry that we didn't notify of the segment completion - this should be pretty rare though
      @Override
      public void segmentComplete(int segment) {
         // This means the consumer was sent the value immediately - this is most likely caused by the transformer
         // didn't have flatMap or anything else fancy (or we had an empty segment)
         if (keyForSegmentCompletion == null) {
            if (trace) {
               log.tracef("Completing segment %s for %s", segment, requestId);
            }
            actualCompleteSegment(segment);
         } else {
            if (trace) {
               log.tracef("Delaying segment completion for %s until key %s is fully consumed for %s", segment,
                     keyForSegmentCompletion, requestId);
            }
            keySegmentCompletions.put(keyForSegmentCompletion, segment);
            keyForSegmentCompletion = null;
         }
      }

      private void actualCompleteSegment(int segment) {
         super.segmentComplete(segment);
         keyPos = 0;
      }

      @Override
      public void segmentLost(int segment) {
         super.segmentLost(segment);
         // We discard any extra values - the super method already discarded the ones found
         keyResetValues();
      }

      @Override
      void prepareResponse(boolean complete) {
         if (complete) {
            assert keySegmentCompletions.isEmpty();
            assert keyCompletionPosition.isEmpty();
         }
         super.prepareResponse(complete);
      }

      void tryPrepareResponse() {
         // We hit the batch size already - so send the prior data
         if (pos == results.length) {
            prepareResponse(false);
         }
      }
   }
}
