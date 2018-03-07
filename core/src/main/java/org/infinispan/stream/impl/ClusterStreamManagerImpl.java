package org.infinispan.stream.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.RangeSet;
import org.infinispan.commons.util.SmallIntSet;
import org.infinispan.commons.util.concurrent.ConcurrentHashSet;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.responses.ValidResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.impl.SingleResponseCollector;
import org.infinispan.remoting.transport.impl.VoidResponseCollector;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import io.reactivex.internal.subscriptions.EmptySubscription;

/**
 * Cluster stream manager that sends all requests using the {@link RpcManager} to do the underlying communications.
 *
 * @param <K> the cache key type
 */
public class ClusterStreamManagerImpl<Original, K> implements ClusterStreamManager<Original, K> {
   protected final Map<String, RequestTracker> currentlyRunning = new ConcurrentHashMap<>();
   protected final Set<Subscriber> iteratorsRunning = new ConcurrentHashSet<>();
   protected final AtomicInteger requestId = new AtomicInteger();
   @Inject protected RpcManager rpc;
   @Inject protected CommandsFactory factory;

   protected RpcOptions rpcOptions;

   protected Address localAddress;

   protected final static Log log = LogFactory.getLog(ClusterStreamManagerImpl.class);
   protected final static boolean trace = log.isTraceEnabled();

   @Start
   public void start() {
      localAddress = rpc.getAddress();
      rpcOptions = new RpcOptions(DeliverOrder.NONE, Long.MAX_VALUE, TimeUnit.DAYS);
   }

   @Override
   public <R> Object remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
         Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
         boolean entryStream, TerminalOperation<Original, R> operation, ResultsCallback<R> callback,
         Predicate<? super R> earlyTerminatePredicate) {
      return commonRemoteStreamOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, entryStream, operation, callback, StreamRequestCommand.Type.TERMINAL,
              earlyTerminatePredicate);
   }

   @Override
   public <R> Object remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
         ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
         boolean includeLoader, boolean entryStream, TerminalOperation<Original, R> operation,
         ResultsCallback<R> callback, Predicate<? super R> earlyTerminatePredicate) {
      return commonRemoteStreamOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, entryStream, operation, callback, StreamRequestCommand.Type.TERMINAL_REHASH,
              earlyTerminatePredicate);
   }

   private <R> Object commonRemoteStreamOperation(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, boolean entryStream, SegmentAwareOperation operation, ResultsCallback<R> callback,
           StreamRequestCommand.Type type, Predicate<? super R> earlyTerminatePredicate) {
      Map<Address, Set<Integer>> targets = determineTargets(ch, segments, callback);
      String id;
      if (!targets.isEmpty()) {
         id = localAddress.toString() + requestId.getAndIncrement();
         log.tracef("Performing remote operations %s for id %s", targets, id);
         RequestTracker<R> tracker = new RequestTracker<>(callback, targets, earlyTerminatePredicate);
         currentlyRunning.put(id, tracker);
         if (parallelDistribution) {
            submitAsyncTasks(id, targets, keysToExclude, parallelStream, keysToInclude, includeLoader, entryStream, type,
                    operation);
         } else {
            for (Map.Entry<Address, Set<Integer>> targetInfo : targets.entrySet()) {
               // TODO: what if this throws exception?
               Set<Integer> targetSegments = targetInfo.getValue();
               Set<K> keysExcluded = determineExcludedKeys(keysToExclude, targetSegments);
               StreamRequestCommand<K> command = factory.buildStreamRequestCommand(id, parallelStream, type,
                     targetSegments, keysToInclude, keysExcluded, includeLoader, entryStream, operation);
               command.setTopologyId(rpc.getTopologyId());
               rpc.blocking(rpc.invokeCommand(targetInfo.getKey(), command, VoidResponseCollector.validOnly(),
                                              rpc.getSyncRpcOptions()));
            }
         }
      } else {
         log.tracef("Not performing remote operation for request as no valid targets for segments %s found", segments);
         id = null;
      }
      return id;
   }

   @Override
   public <R> Object remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
         Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
         boolean entryStream, KeyTrackingTerminalOperation<Original, K, R> operation,
         ResultsCallback<Collection<R>> callback) {
      return commonRemoteStreamOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, entryStream, operation, callback, StreamRequestCommand.Type.TERMINAL_KEY, null);
   }

   @Override
   public Object remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, boolean entryStream, KeyTrackingTerminalOperation<Original, K, ?> operation,
           ResultsCallback<Collection<K>> callback) {
      Map<Address, Set<Integer>> targets = determineTargets(ch, segments, callback);
      String id;
      if (!targets.isEmpty()) {
         id = localAddress.toString() + "-" + requestId.getAndIncrement();
         log.tracef("Performing remote rehash key aware operations %s for id %s", targets, id);
         RequestTracker<Collection<K>> tracker = new RequestTracker<>(callback, targets, null);
         currentlyRunning.put(id, tracker);
         if (parallelDistribution) {
            submitAsyncTasks(id, targets, keysToExclude, parallelStream, keysToInclude, includeLoader, entryStream,
                    StreamRequestCommand.Type.TERMINAL_KEY_REHASH, operation);
         } else {
            for (Map.Entry<Address, Set<Integer>> targetInfo : targets.entrySet()) {
               Address dest = targetInfo.getKey();
               Set<Integer> targetSegments = targetInfo.getValue();
               try {
                  // Keys to exclude is never empty since it utilizes a custom map solution
                  Set<K> keysExcluded = determineExcludedKeys(keysToExclude, targetSegments);
                  log.tracef("Submitting task to %s for %s excluding keys %s", dest, id, keysExcluded);
                  StreamRequestCommand<K> command =
                        factory.buildStreamRequestCommand(id, parallelStream,
                              StreamRequestCommand.Type.TERMINAL_KEY_REHASH, targetSegments, keysToInclude,
                              keysExcluded, includeLoader, entryStream, operation);
                  command.setTopologyId(rpc.getTopologyId());
                  Response response = rpc.blocking(
                     rpc.invokeCommand(dest, command, SingleResponseCollector.validOnly(), rpc.getSyncRpcOptions()));
                  if (!response.isSuccessful()) {
                     log.tracef("Unsuccessful response for %s from %s - making segments %s suspect", id,
                             dest, targetSegments);
                     receiveResponse(id, dest, true, targetSegments, null);
                  }
               } catch (Exception e) {
                  boolean wasSuspect = containedSuspectException(e);

                  if (!wasSuspect) {
                     log.tracef(e, "Encountered exception for %s from %s", id, dest);
                     throw e;
                  } else {
                     log.tracef("Exception from %s contained a SuspectException, making all segments %s suspect",
                             dest, targetSegments);
                     receiveResponse(id, dest, true, targetSegments, null);
                  }
               }
            }
         }
      } else {
         log.tracef("Not performing remote rehash key aware operation for request as no valid targets for segments %s found", segments);
         id = null;
      }
      return id;
   }

   private void submitAsyncTasks(String id, Map<Address, Set<Integer>> targets, Map<Integer, Set<K>> keysToExclude,
                                 boolean parallelStream, Set<K> keysToInclude, boolean includeLoader, boolean entryStream,
                                 StreamRequestCommand.Type type, Object operation) {
      for (Map.Entry<Address, Set<Integer>> targetInfo : targets.entrySet()) {
         Set<Integer> segments = targetInfo.getValue();
         Set<K> keysExcluded = determineExcludedKeys(keysToExclude, segments);
         Address dest = targetInfo.getKey();
         log.tracef("Submitting async task to %s for %s excluding keys %s", dest, id, keysExcluded);
         StreamRequestCommand<K> command = factory.buildStreamRequestCommand(id, parallelStream, type, segments,
               keysToInclude, keysExcluded, includeLoader, entryStream, operation);
         command.setTopologyId(rpc.getTopologyId());
         CompletionStage<ValidResponse> completableFuture =
            rpc.invokeCommand(dest, command, SingleResponseCollector.validOnly(), rpc.getSyncRpcOptions());
         completableFuture.whenComplete((response, e) -> {
            if (e != null) {
               boolean wasSuspect = containedSuspectException(e);

               if (!wasSuspect) {
                  log.tracef(e, "Encountered exception for %s from %s", id, targetInfo.getKey());
                  RequestTracker tracker = currentlyRunning.get(id);
                  if (tracker != null) {
                     markTrackerWithException(tracker, dest, e, id);
                  } else {
                     log.warnf("Unhandled remote stream exception encountered", e);
                  }
               } else {
                  log.tracef("Exception contained a SuspectException, making all segments %s suspect",
                             targetInfo.getValue());
                  receiveResponse(id, targetInfo.getKey(), true, targetInfo.getValue(), null);
               }
            } else if (response != null) {
               if (!response.isSuccessful()) {
                  log.tracef("Unsuccessful response for %s from %s - making segments suspect", id, targetInfo.getKey());
                  receiveResponse(id, targetInfo.getKey(), true, targetInfo.getValue(), null);
               }
            }
         });
      }
   }

   private boolean containedSuspectException(Throwable e) {
      Throwable cause = e;
      boolean wasSuspect = false;
      // Unwrap the exception
      do {
         if (cause instanceof SuspectException) {
            wasSuspect = true;
            break;
         }
      } while ((cause = cause.getCause()) != null);

      return wasSuspect;
   }

   protected static void markTrackerWithException(RequestTracker<?> tracker, Address dest, Throwable e, Object uuid) {
      log.tracef("Marking tracker to have exception");
      tracker.throwable = e;
      if (dest == null || tracker.lastResult(dest, null)) {
         if (uuid != null) {
            log.tracef("Tracker %s completed with exception, waking sleepers!", uuid);
         } else {
            log.trace("Tracker completed due to outside cause, waking sleepers! ");
         }
         tracker.completionLock.lock();
         try {
            tracker.completionCondition.signalAll();
         } finally {
            tracker.completionLock.unlock();
         }
      }
   }

   private Set<K> determineExcludedKeys(Map<Integer, Set<K>> keysToExclude, Set<Integer> segmentsToUse) {
      if (keysToExclude.isEmpty()) {
         return Collections.emptySet();
      }

      // Special map only supports get operations
      return segmentsToUse.stream().flatMap(s -> {
         Set<K> keysForSegment = keysToExclude.get(s);
         if (keysForSegment != null) {
            return keysForSegment.stream();
         }
         return null;
      }).collect(Collectors.toSet());
   }

   // TODO: we could have this method return a Stream etc. so it doesn't have to iterate upon keys multiple times (helps rehash and tx)
   private Set<K> determineExcludedKeys(IntFunction<Set<K>> keysToExclude, IntSet segmentsToUse) {
      if (keysToExclude == null) {
         return Collections.emptySet();
      }

      // Special map only supports get operations
      return segmentsToUse.intStream().mapToObj(s -> {
         Set<K> keysForSegment = keysToExclude.apply(s);
         if (keysForSegment != null) {
            return keysForSegment.stream();
         }
         return null;
      }).flatMap(Function.identity()).collect(Collectors.toSet());
   }

   private Map<Address, Set<Integer>> determineTargets(ConsistentHash ch, Set<Integer> segments, ResultsCallback<?> callback) {
      if (segments == null) {
         segments = new RangeSet(ch.getNumSegments());
      }
      // This has to be a concurrent hash map in case if a node completes operation while we are still iterating
      // over the map and submitting to others
      Map<Address, Set<Integer>> targets = new ConcurrentHashMap<>();
      for (Integer segment : segments) {
         Address owner = ch.locatePrimaryOwnerForSegment(segment);
         if (owner == null) {
            callback.onSegmentsLost(Collections.singleton(segment));
            callback.requestFutureTopology();
         } else if (!owner.equals(localAddress)) {
            targets.computeIfAbsent(owner, t -> new SmallIntSet()).add(segment);
         }
      }
      return targets;
   }

   @Override
   public boolean isComplete(Object id) {
      return !currentlyRunning.containsKey(id);
   }

   @Override
   public boolean awaitCompletion(Object id, long time, TimeUnit unit) throws InterruptedException {
      if (time <= 0) {
         throw new IllegalArgumentException("Time must be greater than 0");
      }
      Objects.requireNonNull(id, "Identifier must be non null");

      log.tracef("Awaiting completion of %s", id);

      boolean completed = false;
      long target = System.nanoTime() + unit.toNanos(time);
      Throwable throwable = null;
      while (target - System.nanoTime() > 0) {
         RequestTracker tracker = currentlyRunning.get(id);
         if (tracker == null) {
            completed = true;
            break;
         }
         if ((throwable = tracker.throwable) != null) {
            break;
         }
         tracker.completionLock.lock();
         try {
            // Check inside lock again just in case if we had a concurrent notification before we got
            // into the lock
            if (!currentlyRunning.containsKey(id)) {
               completed = true;
               throwable = tracker.throwable;
               break;
            }
            if (!tracker.completionCondition.await(target - System.nanoTime(), TimeUnit.NANOSECONDS)) {
               throwable = tracker.throwable;
               completed = false;
               break;
            }
         } finally {
            tracker.completionLock.unlock();
         }
      }
      log.tracef("Returning back to caller due to %s being completed: %s", id, completed);
      if (throwable != null) {
         if (throwable instanceof RuntimeException) {
            throw ((RuntimeException) throwable);
         }
         throw new CacheException(throwable);
      }

      return completed;
   }

   @Override
   public void forgetOperation(Object id) {
      if (id != null) {
         RequestTracker<?> tracker = currentlyRunning.remove(id);
         if (tracker != null) {
            tracker.completionLock.lock();
            try {
               tracker.completionCondition.signalAll();
            } finally {
               tracker.completionLock.unlock();
            }
         }
      }
   }

   @Override
   public <R1> boolean receiveResponse(Object id, Address origin, boolean complete, Set<Integer> missingSegments,
                                    R1 response) {
      log.tracef("Received response from %s with a completed response %s for id %s with %s suspected segments.", origin,
              complete, id, missingSegments);
      RequestTracker tracker = currentlyRunning.get(id);

      if (tracker != null) {
         boolean notify = false;
         // TODO: need to reorganize the tracker to reduce synchronization so it only contains missing segments
         // and completing the tracker
         synchronized(tracker) {
            if (tracker.awaitingResponse.containsKey(origin)) {
               if (!missingSegments.isEmpty()) {
                  tracker.missingSegments(missingSegments);
               }
               if (complete) {
                  notify = tracker.lastResult(origin, response);
               } else {
                  tracker.intermediateResults(origin, response);
               }
            }
         }
         if (notify) {
            log.tracef("Marking %s as completed!", id);
            tracker.completionLock.lock();
            try {
               currentlyRunning.remove(id);
               tracker.completionCondition.signalAll();
            } finally {
               tracker.completionLock.unlock();
            }
         }
         return !notify;
      } else {
         log.tracef("Ignoring response as we already received a completed response for %s from %s", id, origin);
         return false;
      }
   }

   @Override
   public <E> RemoteIteratorPublisher<E> remoteIterationPublisher(boolean parallelStream,
         Supplier<Map.Entry<Address, IntSet>> targets, Set<K> keysToInclude, IntFunction<Set<K>> keysToExclude,
         boolean includeLoader, boolean entryStream, Iterable<IntermediateOperation> intermediateOperations) {

      return new RemoteIteratorPublisherImpl<>(parallelStream, targets, keysToInclude, keysToExclude, includeLoader,
            entryStream, intermediateOperations);
   }

   private class RemoteIteratorPublisherImpl<V> implements RemoteIteratorPublisher<V> {
      private final boolean parallelStream;
      private final Supplier<Map.Entry<Address, IntSet>> targets;
      private final Set<K> keysToInclude;
      private final IntFunction<Set<K>> keysToExclude;
      private final boolean includeLoader;
      private final boolean entryStream;
      private final Iterable<IntermediateOperation> intermediateOperations;

      RemoteIteratorPublisherImpl(boolean parallelStream, Supplier<Map.Entry<Address, IntSet>> targets,
            Set<K> keysToInclude, IntFunction<Set<K>> keysToExclude, boolean includeLoader,
            boolean entryStream, Iterable<IntermediateOperation> intermediateOperations) {
         this.parallelStream = parallelStream;
         this.targets = targets;
         this.keysToInclude = keysToInclude;
         this.keysToExclude = keysToExclude;
         this.includeLoader = includeLoader;
         this.entryStream = entryStream;
         this.intermediateOperations = intermediateOperations;
      }

      @Override
      public void subscribe(Subscriber<? super V> s, Consumer<? super Supplier<PrimitiveIterator.OfInt>> onSegmentsComplete,
            Consumer<? super Supplier<PrimitiveIterator.OfInt>> onLostSegments) {
         Map.Entry<Address, IntSet> target = targets.get();
         if (target == null) {
            EmptySubscription.complete(s);
         } else {
            String id = localAddress.toString() + "-" + requestId.getAndIncrement();
            if (trace) {
               log.tracef("Starting request: %s", id);
            }
            iteratorsRunning.add(s);
            s.onSubscribe(new ClusterStreamSubscription<V>(s, this, onSegmentsComplete, onLostSegments, id, target));
         }
      }
   }

   private class ClusterStreamSubscription<V> implements Subscription {

      private final Subscriber<? super V> s;
      private final RemoteIteratorPublisherImpl<V> publisher;
      private final Consumer<? super Supplier<PrimitiveIterator.OfInt>> onSegmentsComplete;
      private final Consumer<? super Supplier<PrimitiveIterator.OfInt>> onSegmentsLost;
      private final String id;

      private final AtomicLong requestedAmount = new AtomicLong();
      private final AtomicBoolean pendingRequest = new AtomicBoolean();

      private volatile AtomicReference<Map.Entry<Address, IntSet>> currentTarget;
      private volatile boolean alreadyCreated;

      ClusterStreamSubscription(Subscriber<? super V> s, RemoteIteratorPublisherImpl<V> publisher,
            Consumer<? super Supplier<PrimitiveIterator.OfInt>> onSegmentsComplete,
            Consumer<? super Supplier<PrimitiveIterator.OfInt>> onSegmentsLost,
            String id, Map.Entry<Address, IntSet> currentTarget) {
         this.s = s;
         this.publisher = publisher;
         this.onSegmentsComplete = onSegmentsComplete;
         this.onSegmentsLost = onSegmentsLost;
         this.id = id;
         // We assume the map has at least one otherwise this subscription wouldn't be created
         this.currentTarget = new AtomicReference<>(currentTarget);
      }

      @Override
      public void request(long n) {
         // If current target is null it means we completed
         if (currentTarget == null) {
            return;
         }
         if (n <= 0) {
            throw new IllegalArgumentException("request amount must be greater than 0");
         }
         requestedAmount.addAndGet(n);
         // If there is no pending request we can submit a new one
         if (!pendingRequest.getAndSet(true)) {
            // We can only send the batch amount after we have confirmed that we will send the request
            // otherwise we could request too much since this requestedAmount is not decremented until
            // all entries have been sent via onNext. However the amount is decremented before updating pendingRequest
            sendRequest(requestedAmount.get());
         }
      }

      StreamIteratorNextCommand getCommand(IntSet segments, long batchAmount) {
         if (alreadyCreated) {
            return factory.buildStreamIteratorNextCommand(id, batchAmount);
         } else {
            alreadyCreated = true;
            return factory.buildStreamIteratorRequestCommand(id,
                  publisher.parallelStream, segments, publisher.keysToInclude,
                  determineExcludedKeys(publisher.keysToExclude, segments), publisher.includeLoader,
                  publisher.entryStream, publisher.intermediateOperations, batchAmount);
         }
      }

      private void sendRequest(long batchAmount) {
         // Copy the variable in case if we are closed concurrently - also this double check works for resubmission
         Map.Entry<Address, IntSet> target = currentTarget.get();
         if (target != null) {
            IntSet segments = target.getValue();
            if (trace) {
               log.tracef("Request: %s is requesting %d more entries from %s in segments %s", id, batchAmount, target, segments);
            }
            Address sendee = target.getKey();
            StreamIteratorNextCommand command = getCommand(segments, batchAmount);
            command.setTopologyId(rpc.getTopologyId());
            CompletionStage<ValidResponse> rpcStage =
               rpc.invokeCommand(sendee, command, SingleResponseCollector.validOnly(), rpcOptions);
            rpcStage.whenComplete((r, t) -> {
               if (t != null) {
                  handleThrowable(t, target);
               } else {
                  try {
                     if (r instanceof SuccessfulResponse) {
                        IteratorResponse<V> iteratorResponse = (IteratorResponse) r.getResponseValue();
                        if (trace) {
                           log.tracef("Received valid response %s for id %s from node %s", iteratorResponse, id, target.getKey());
                        }
                        Spliterator<V> spliterator = iteratorResponse.getSpliterator();
                        long returnedAmount = spliterator.getExactSizeIfKnown();
                        if (trace) {
                           log.tracef("Received %d entries for id %s from %s", returnedAmount, id, sendee);
                        }
                        spliterator.forEachRemaining(s::onNext);

                        if (iteratorResponse.isComplete()) {
                           Set<Integer> lostSegments = iteratorResponse.getSuspectedSegments();
                           if (lostSegments.isEmpty()) {
                              onSegmentsComplete.accept((Supplier<PrimitiveIterator.OfInt>) segments::iterator);
                           } else {
                              onSegmentsLost.accept((Supplier<PrimitiveIterator.OfInt>)
                                    () -> lostSegments.stream().mapToInt(Integer::intValue).iterator());

                              if (lostSegments.size() != segments.size()) {
                                 // TODO: need to convert response to return IntSet
                                 onSegmentsComplete.accept((Supplier<PrimitiveIterator.OfInt>)
                                       () -> segments.intStream()
                                             .filter(s -> !lostSegments.contains(s))
                                             .iterator());
                              }
                           }

                           Map.Entry<Address, IntSet> nextTarget = publisher.targets.get();

                           if (nextTarget != null) {
                              alreadyCreated = false;
                              // Only set if target is still the same
                              // No other thread can be here (see pendingRequest)
                              // so if it fails it means it must be closed (ie. null)
                              currentTarget.compareAndSet(target, nextTarget);
                           } else {
                              currentTarget.set(null);
                              // No more targets means this subscription is done
                              completed();
                              return;
                           }
                        }

                        long remaining = requestedAmount.addAndGet(-returnedAmount);
                        if (remaining > 0) {
                           // Either more was requested while we were processing or we didn't return enough, so
                           // try again
                           sendRequest(remaining);
                        } else {
                           pendingRequest.set(false);
                           // We have to recheck just in case if there was another thread that added to request amount
                           // but was unable to acquire pending request (otherwise no pending request will be sent)
                           remaining = requestedAmount.get();
                           if (remaining > 0 && !pendingRequest.getAndSet(true)) {
                              sendRequest(remaining);
                           }
                        }
                     } else {
                        handleThrowable(new IllegalArgumentException("Unsupported response received: " + r), target);
                     }
                  } catch (Throwable throwable) {
                     // This block is for general programming issues to notify directly to user thread
                     cancel();
                     s.onError(throwable);
                  }
               }
            });
         }
      }

      private void handleThrowable(Throwable t, Map.Entry<Address, IntSet> target) {
         cancel();
         // Most likely SuspectException will be wrapped in CompletionException
         if (t instanceof SuspectException || t.getCause() instanceof SuspectException) {
            if (trace) {
               log.tracef("Received suspect exception for id %s from node %s when requesting segments %s", id,
                     target.getKey(), target.getValue());
            }
            onSegmentsLost.accept((Supplier<PrimitiveIterator.OfInt>) () -> target.getValue().iterator());
            // We then have to tell the subscriber we completed - even though we lost segments
            s.onComplete();
         } else {
            if (trace) {
               log.tracef(t, "Received exception for id %s from node %s when requesting segments %s", id, target.getKey(),
                     target.getValue());
            }
            s.onError(t);
         }
      }

      @Override
      public void cancel() {
         Map.Entry<Address, IntSet> target = currentTarget.getAndSet(null);
         if (target != null && alreadyCreated) {
            Address targetNode = target.getKey();
            CompletionStage<ValidResponse> rpcStage =
               rpc.invokeCommand(targetNode, factory.buildStreamIteratorCloseCommand(id),
                                 SingleResponseCollector.validOnly(), rpcOptions);
            if (trace) {
               rpcStage.exceptionally(t -> {
                  log.tracef(t, "Unable to close iterator on %s for requestId %s", targetNode, requestId);
                  return null;
               });
            }
         }
         iteratorsRunning.remove(s);
      }

      private void completed() {
         if (trace) {
            log.tracef("Processor completed for request: %s", id);
         }
         cancel();
         s.onComplete();
      }
   }

   static class RequestTracker<R> {
      final ResultsCallback<R> callback;
      final Map<Address, Set<Integer>> awaitingResponse;
      final Lock completionLock = new ReentrantLock();
      final Condition completionCondition = completionLock.newCondition();
      final Predicate<? super R> earlyTerminatePredicate;

      Set<Integer> missingSegments;

      volatile Throwable throwable;

      RequestTracker(ResultsCallback<R> callback, Map<Address, Set<Integer>> awaitingResponse,
                     Predicate<? super R> earlyTerminatePredicate) {
         this.callback = callback;
         this.awaitingResponse = awaitingResponse;
         this.earlyTerminatePredicate = earlyTerminatePredicate;
      }

      public void intermediateResults(Address origin, R intermediateResult) {
         callback.onIntermediateResult(origin, intermediateResult);
      }

      /**
       * @param origin
       * @param result
       * @return Whether this was the last expected response
       */
      public boolean lastResult(Address origin, R result) {
         Set<Integer> completedSegments = awaitingResponse.get(origin);
         if (missingSegments != null) {
            completedSegments.removeAll(missingSegments);
         }
         callback.onCompletion(origin, completedSegments, result);
         synchronized (this) {
            if (earlyTerminatePredicate != null && result != null && earlyTerminatePredicate.test(result)) {
               awaitingResponse.clear();
            } else {
               awaitingResponse.remove(origin);
            }
            return awaitingResponse.isEmpty();
         }
      }

      public void missingSegments(Set<Integer> segments) {
         synchronized (this) {
            if (missingSegments == null) {
               missingSegments = segments;
            } else {
               missingSegments.addAll(segments);
            }
         }
         callback.onSegmentsLost(segments);
      }
   }
}
