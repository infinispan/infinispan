package org.infinispan.stream.impl;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commons.CacheException;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.PartitionStatusChanged;
import org.infinispan.notifications.cachelistener.event.PartitionStatusChangedEvent;
import org.infinispan.partitionhandling.AvailabilityException;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.util.concurrent.ConcurrentHashSet;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Cluster stream manager that sends all requests using the {@link RpcManager} to do the underlying communications.
 * @param <K> the cache key type
 */
public class ClusterStreamManagerImpl<K> implements ClusterStreamManager<K> {
   protected final Map<UUID, RequestTracker> currentlyRunning = new ConcurrentHashMap<>();
   protected RpcManager rpc;
   protected CommandsFactory factory;

   protected Address localAddress;

   protected final static Log log = LogFactory.getLog(ClusterStreamManagerImpl.class);

   @Inject
   public void inject(RpcManager rpc, CommandsFactory factory) {
      this.rpc = rpc;
      this.factory = factory;
   }

   @Start
   public void start() {
      localAddress = rpc.getAddress();
   }

   @Override
   public <R> UUID remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
           Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
           TerminalOperation<R> operation, ResultsCallback<R> callback, Predicate<? super R> earlyTerminatePredicate) {
      return remoteStreamIgnoreKeyOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, operation, callback, StreamRequestCommand.Type.TERMINAL,
              earlyTerminatePredicate);
   }

   @Override
   public <R> UUID remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, TerminalOperation<R> operation, ResultsCallback<R> callback,
           Predicate<? super R> earlyTerminatePredicate) {
      return remoteStreamIgnoreKeyOperation(parallelDistribution, parallelStream, ch, segments, keysToInclude,
              keysToExclude, includeLoader, operation, callback, StreamRequestCommand.Type.TERMINAL_REHASH,
              earlyTerminatePredicate);
   }

   private <R> UUID remoteStreamIgnoreKeyOperation(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, TerminalOperation<R> operation, ResultsCallback<R> callback,
           StreamRequestCommand.Type type, Predicate<? super R> earlyTerminatePredicate) {
      Map<Address, Set<Integer>> targets = determineTargets(ch, segments);
      UUID uuid = UUID.randomUUID();
      if (!targets.isEmpty()) {
         log.tracef("Performing remote operations %s for id %s", targets, uuid);
         RequestTracker<R> tracker = new RequestTracker<>(callback, targets, earlyTerminatePredicate);
         currentlyRunning.put(uuid, tracker);
         if (parallelDistribution) {
            submitAsyncTasks(uuid, targets, keysToExclude, parallelStream, keysToInclude, includeLoader, type,
                    operation);
         } else {
            for (Map.Entry<Address, Set<Integer>> targetInfo : targets.entrySet()) {
               // TODO: what if this throws exception?
               Set<Integer> targetSegments = targetInfo.getValue();
               Set<K> keysExcluded = determineExcludedKeys(keysToExclude, targetSegments);
               rpc.invokeRemotely(Collections.singleton(targetInfo.getKey()),
                       factory.buildStreamRequestCommand(uuid, parallelStream, type, targetSegments, keysToInclude,
                               keysExcluded, includeLoader, operation), rpc.getDefaultRpcOptions(true));
            }
         }
      } else {
         log.tracef("Not performing any remote operations for id %s as no valid targets found", uuid);
      }
      return uuid;
   }

   @Override
   public <R> UUID remoteStreamOperation(boolean parallelDistribution, boolean parallelStream, ConsistentHash ch,
           Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude, boolean includeLoader,
           KeyTrackingTerminalOperation<K, R, ?> operation, ResultsCallback<Collection<R>> callback) {
      Map<Address, Set<Integer>> targets = determineTargets(ch, segments);
      UUID uuid = UUID.randomUUID();
      if (!targets.isEmpty()) {
         log.tracef("Performing remote key aware operations %s for id %s", targets, uuid);
         RequestTracker<Collection<R>> tracker = new RequestTracker<>(callback, targets, null);
         currentlyRunning.put(uuid, tracker);
         if (parallelDistribution) {
            submitAsyncTasks(uuid, targets, keysToExclude, parallelStream, keysToInclude, includeLoader,
                    StreamRequestCommand.Type.TERMINAL_KEY, operation);
         } else {
            for (Map.Entry<Address, Set<Integer>> targetInfo : targets.entrySet()) {
               Set<Integer> targetSegments = targetInfo.getValue();
               Set<K> keysExcluded = determineExcludedKeys(keysToExclude, targetSegments);
               rpc.invokeRemotely(Collections.singleton(targetInfo.getKey()), factory.buildStreamRequestCommand(uuid,
                       parallelStream, StreamRequestCommand.Type.TERMINAL_KEY, targetSegments, keysToInclude,
                       keysExcluded, includeLoader, operation), rpc.getDefaultRpcOptions(true));
            }
         }
      }
      return uuid;
   }

   @Override
   public <R2> UUID remoteStreamOperationRehashAware(boolean parallelDistribution, boolean parallelStream,
           ConsistentHash ch, Set<Integer> segments, Set<K> keysToInclude, Map<Integer, Set<K>> keysToExclude,
           boolean includeLoader, KeyTrackingTerminalOperation<K, ?, R2> operation,
           ResultsCallback<Map<K, R2>> callback) {
      Map<Address, Set<Integer>> targets = determineTargets(ch, segments);
      UUID uuid = UUID.randomUUID();
      if (!targets.isEmpty()) {
         log.tracef("Performing remote rehash key aware operations %s for id %s", targets, uuid);
         RequestTracker<Map<K, R2>> tracker = new RequestTracker<>(callback, targets, null);
         currentlyRunning.put(uuid, tracker);
         if (parallelDistribution) {
            submitAsyncTasks(uuid, targets, keysToExclude, parallelStream, keysToInclude, includeLoader,
                    StreamRequestCommand.Type.TERMINAL_KEY_REHASH, operation);
         } else {
            for (Map.Entry<Address, Set<Integer>> targetInfo : targets.entrySet()) {
               Address dest = targetInfo.getKey();
               Set<Integer> targetSegments = targetInfo.getValue();
               try {
                  // Keys to exclude is never empty since it utilizes a custom map solution
                  Set<K> keysExcluded = determineExcludedKeys(keysToExclude, targetSegments);
                  log.tracef("Submitting task to %s for %s excluding keys %s", dest, uuid, keysExcluded);
                  Response response = rpc.invokeRemotely(Collections.singleton(dest), factory.buildStreamRequestCommand(
                          uuid, parallelStream, StreamRequestCommand.Type.TERMINAL_KEY_REHASH, targetSegments,
                          keysToInclude, keysExcluded, includeLoader, operation),
                          rpc.getDefaultRpcOptions(true)).values().iterator().next();
                  if (!response.isSuccessful()) {
                     log.tracef("Unsuccessful response for %s from %s - making segments %s suspect", uuid,
                             dest, targetSegments);
                     receiveResponse(uuid, dest, true, targetSegments, null);
                  }
               } catch (Exception e) {
                  Throwable cause = e;
                  boolean wasSuspect = false;
                  // Unwrap exception
                  do {
                     if (cause instanceof SuspectException) {
                        log.tracef("Exception from %s contained a SuspectException, making all segments %s suspect",
                                dest, targetSegments);
                        receiveResponse(uuid, dest, true, targetSegments, null);
                        wasSuspect = true;
                        break;
                     }
                  } while ((cause = cause.getCause()) != null);

                  if (!wasSuspect) {
                     log.tracef(e, "Encounted exception for %s from %s", uuid, dest);
                     throw e;
                  }
               }
            }
         }
      }
      return uuid;
   }

   private void submitAsyncTasks(UUID uuid, Map<Address, Set<Integer>> targets, Map<Integer, Set<K>> keysToExclude,
                                 boolean parallelStream, Set<K> keysToInclude, boolean includeLoader,
                                 StreamRequestCommand.Type type, Object operation) {
      for (Map.Entry<Address, Set<Integer>> targetInfo : targets.entrySet()) {
         Set<Integer> segments = targetInfo.getValue();
         Set<K> keysExcluded = determineExcludedKeys(keysToExclude, segments);
         Address dest = targetInfo.getKey();
         log.tracef("Submitting async task to %s for %s excluding keys %s", dest, uuid, keysExcluded);
         CompletableFuture<Map<Address, Response>> completableFuture = rpc.invokeRemotelyAsync(
                 Collections.singleton(dest), factory.buildStreamRequestCommand(uuid, parallelStream, type, segments,
                         keysToInclude, keysExcluded, includeLoader, operation),
                 rpc.getDefaultRpcOptions(true));
         completableFuture.whenComplete((v, e) -> {
            if (v != null) {
               Response response = v.values().iterator().next();
               if (!response.isSuccessful()) {
                  log.tracef("Unsuccessful response for %s from %s - making segments suspect", uuid, targetInfo.getKey());
                  receiveResponse(uuid, targetInfo.getKey(), true, targetInfo.getValue(), null);
               }
            } else if (e != null) {
               Throwable cause = e;
               boolean wasSuspect = false;
               // Unwrap the exception
               do {
                  if (cause instanceof SuspectException) {
                     log.tracef("Exception contained a SuspectException, making all segments %s suspect",
                             targetInfo.getValue());
                     receiveResponse(uuid, targetInfo.getKey(), true, targetInfo.getValue(), null);
                     wasSuspect = true;
                     break;
                  }
               } while ((cause = cause.getCause()) != null);

               if (!wasSuspect) {
                  log.tracef(e, "Encounted exception for %s from %s", uuid, targetInfo.getKey());
                  RequestTracker tracker = currentlyRunning.get(uuid);
                  if (tracker != null) {
                     markTrackerWithException(tracker, dest, e, uuid);
                  } else {
                     log.warnf("Unhandled remote stream exception encountered", e);
                  }
               }
            }
         });
      }
   }

   protected static void markTrackerWithException(RequestTracker<?> tracker, Address dest, Throwable e, UUID uuid) {
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

   private Map<Address, Set<Integer>> determineTargets(ConsistentHash ch, Set<Integer> segments) {
      // This has to be a concurrent hash map in case if a node completes operation while we are still iterating
      // over the map and submitting to others
      Map<Address, Set<Integer>> targets = new ConcurrentHashMap<>();
      List<Address> addresses = ch.getMembers();
      for (Address address : addresses) {
         if (address.equals(localAddress)) {
            continue;
         }
         // TODO: we should add a new primary segments for all owners method
         Set<Integer> theirSegments = ch.getPrimarySegmentsForOwner(address);
         if (segments != null && theirSegments.retainAll(segments) && theirSegments.isEmpty()) {
            continue;
         }
         targets.put(address, theirSegments);
      }
      return targets;
   }

   @Override
   public boolean isComplete(UUID id) {
      return !currentlyRunning.containsKey(id);
   }

   @Override
   public boolean awaitCompletion(UUID id, long time, TimeUnit unit) throws InterruptedException {
      if (time <= 0) {
         throw new IllegalArgumentException("Time must be greater than 0");
      }

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
   public void forgetOperation(UUID id) {
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

   @Override
   public <R1> boolean receiveResponse(UUID id, Address origin, boolean complete, Set<Integer> missingSegments,
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
       *
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
            if (earlyTerminatePredicate != null  && earlyTerminatePredicate.test(result)) {
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
