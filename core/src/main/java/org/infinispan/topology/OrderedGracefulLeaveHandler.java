package org.infinispan.topology;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

import org.infinispan.commons.time.TimeService;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferTracker;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Serializes graceful leave operations on the coordinator to prevent data loss during concurrent scale-down.
 *
 * <p>
 * When multiple nodes leave simultaneously (e.g., Kubernetes scaling down a StatefulSet), each leave triggers a rebalance
 * that redistributes data to surviving nodes. Without serializing the operations, all leaves execute before any rebalance
 * completes, causing segments to lose all owners.
 * </p>
 *
 * <p>
 * This handler maintains a per-cache chain of {@link CompletableFuture}s. Each leave waits for the previous leave's rebalance
 * to finish before proceeding:
 * </p>
 * <pre>
 *   Leave B --(doLeave + wait rebalance)--> Leave C --(doLeave + wait rebalance)--> ...
 * </pre>
 *
 * <p>
 * Only used when the leaving node requests a graceful stop with a positive timeout (via
 * {@link org.infinispan.commands.topology.CacheLeaveCommand}). Abrupt leaves (view change, kill) bypass this handler
 * entirely.
 * </p>
 *
 * @since 16.2
 * @see ClusterTopologyManagerImpl#handleLeave(String, Address, long, TimeUnit)
 * @see StateTransferTracker
 */
@Scope(Scopes.GLOBAL)
public class OrderedGracefulLeaveHandler {

   private static final Log LOG = LogFactory.getLog(OrderedGracefulLeaveHandler.class);

   private final Map<String, LeaveChain> chains = new ConcurrentHashMap<>();

   @Inject
   protected BlockingManager blockingManager;

   @Inject
   protected TimeService timeService;

   @Inject
   protected StateTransferTracker tracker;

   /**
    * Enqueues a graceful leave for the given cache, serialized behind any pending leaves.
    *
    * @param cacheName the cache the node is leaving
    * @param leaver    the address of the leaving node
    * @param timeout   maximum time to wait for the rebalance triggered by this leave
    * @param unit      time unit for the timeout
    * @param status    the coordinator-side cache status, used to execute the leave and read topology state
    * @return a stage that completes when this leave's rebalance finishes (or the timeout expires)
    */
   public CompletionStage<Void> enqueue(String cacheName, Address leaver, long timeout, TimeUnit unit, ClusterCacheStatus status) {
      LeaveChain chain = chains.computeIfAbsent(cacheName, k -> new LeaveChain(tracker.forCache(cacheName)));
      return chain.append(leaver, timeout, unit, status);
   }

   /**
    * Removes the leave chain for a cache. Called when the last member leaves and the cache status is removed.
    *
    * @param cacheName the cache to clean up
    */
   public void remove(String cacheName) {
      chains.remove(cacheName);
   }

   /**
    * Waits for all in-flight graceful leave chains to complete.
    *
    * <p>
    * The method ensures that all currently pending leave requests are consumed before returning. Concurrent leave requests
    * included <i>while</i> draining are also included in the consuming list. The method waits until all the requests
    * and associated state transfer complete, or a timeout elapses.
    * </p>
    *
    * @param timeout maximum time to wait for all chains to complete
    * @param unit    time unit for the timeout
    * @return {@code true} if all chains completed before the deadline; {@code false} if the timeout expired
    * @throws InterruptedException if the calling thread is interrupted while waiting
    */
   public boolean drainAll(long timeout, TimeUnit unit) throws InterruptedException {
      final long deadline = timeService.expectedEndTime(timeout, unit);

      LOG.tracef("Draining graceful leavers in (%d, %s)", timeout, unit);
      for (;;) {
         long remaining = timeService.remainingTime(deadline, TimeUnit.MILLISECONDS);
         if (remaining <= 0)
            return false;

         CompletableFuture<?>[] tails = chains.values().stream()
               .map(LeaveChain::snapshot)
               .filter(t -> !t.isDone())
               .toArray(CompletableFuture[]::new);

         if (tails.length == 0)
            return true;

         try {
            CompletableFuture.allOf(tails).get(remaining, TimeUnit.MILLISECONDS);
         } catch (ExecutionException e) {
            // We only log as debug and let it continue to drain.
            LOG.debugf(e, "Failed while draining the ordered leave chains");
         } catch (TimeoutException e) {
            return false;
         }
      }
   }

   /**
    * Per-cache chain that serializes leave operations using {@link CompletableFuture} linking.
    *
    * <p>
    * Each call to {@link #append} chains a new leave behind the previous one. The chain ensures that {@code doLeave(B)}
    * and its resulting rebalance complete before {@code doLeave(C)} begins.
    * </p>
    */
   private final class LeaveChain {
      private final ReentrantLock lock = new ReentrantLock();
      private final StateTransferTracker.CacheStateTransferTracker tracker;

      // The tail of the chain. Each new leave appends behind this future.
      // Initialized as complete so the first leave executes immediately.
      private CompletableFuture<Void> tail = CompletableFutures.completedNull();

      private LeaveChain(StateTransferTracker.CacheStateTransferTracker tracker) {
         this.tracker = tracker;
      }

      /**
       * Appends a leave operation to the chain.
       *
       * <p>
       * The returned stage completes when:
       * <ol>
       *   <li>All previously enqueued leaves have finished (including their rebalances).</li>
       *   <li>This leave's {@code doLeave} has executed and the resulting rebalance has reached a stable topology.</li>
       * </ol>
       *
       * If the timeout expires before completion, the stage completes exceptionally but the chain still advances (via
       * {@code whenComplete}) to avoid blocking subsequent leaves.
       * </p>
       *
       * @param leaver  the address of the leaving node
       * @param timeout maximum wait time for this leave (includes queue wait + rebalance)
       * @param unit    time unit for the timeout
       * @param status  the coordinator-side cache status
       * @return a stage that completes when the leave and its rebalance are done
       */
      public CompletionStage<Void> append(Address leaver, long timeout, TimeUnit unit, ClusterCacheStatus status) {
         LOG.tracef("Adding graceful leaver %s to chain (%d, %s) with %s", leaver, timeout, unit, status);
         acquireLock();
         try {
            CompletableFuture<Void> previous = tail;
            CompletableFuture<Void> next = new CompletableFuture<>();
            tail = next;

            return previous
                  // Do leave will acquire locks on the cache status.
                  .thenComposeAsync(ignored -> doLeaveAndWaitRebalance(leaver, status), blockingManager.asExecutor(status.getCacheName() + "-leaver-" + leaver))
                  .orTimeout(timeout, unit)
                  .whenComplete((ignored, t) -> next.complete(null));
         } finally {
            releaseLock();
         }
      }

      private CompletionStage<Void> doLeaveAndWaitRebalance(Address leaver, ClusterCacheStatus status) {
         status.doLeave(leaver);
         // Read the topology ID AFTER doLeave.
         // doLeave internally calls updateCurrentTopology (which creates a NO_REBALANCE topology reflecting the new membership)
         // followed by startQueuedRebalance (which creates a READ_OLD_WRITE_ALL topology to begin data redistribution).
         // Reading after doLeave captures the rebalance-start topology ID.
         //
         // If we read BEFORE doLeave, the predicate would be satisfied by the intermediate NO_REBALANCE topology from
         // updateCurrentTopology, which would still lead to data loss with concurrent leaves.
         int previous = status.getCurrentTopology().getTopologyId();

         // Wait for a stable topology (pendingCH == null) with an ID >= postLeave.
         // - Rebalance case: postLeave is the READ_OLD_WRITE_ALL topology. The rebalance progresses through
         //   phases until NO_REBALANCE, which has a higher ID. The >= predicate passes.
         // - No-rebalance case (e.g., balanced CH unchanged): postLeave is already the final NO_REBALANCE
         //   topology. The >= predicate passes immediately once the tracker processes it.
         return tracker.onStateTransferCompleted((ct, t) -> {
            // Complete in case of failures.
            if (t != null)
               return true;

            return ct.getTopologyId() >= previous;
         });
      }

      private CompletableFuture<Void> snapshot() {
         acquireLock();
         try {
            return tail;
         } finally {
            releaseLock();
         }
      }

      private void acquireLock() {
         lock.lock();
      }

      private void releaseLock() {
         lock.unlock();
      }
   }
}
