package org.infinispan.statetransfer;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.annotations.GuardedBy;

import com.google.errorprone.annotations.ThreadSafe;

/**
 * Tracks the progress of state transfer for a specific cache.
 *
 * <p>
 * This component coordinates the completion of three distinct phases required for a state transfer to be considered
 * fully finished:
 * <ul>
 * <li><b>State Consumer:</b> The phase where this node requests and applies state from other nodes.</li>
 * <li><b>State Provider:</b> The phase where this node provides state to other nodes.</li>
 * <li><b>Stable Topology:</b> The installation of a cache topology with no pending consistent hash ({@code pendingCH == null}).</li>
 * </ul>
 * </p>
 *
 * <p>
 * Listeners can be registered to wait for the completion of the state transfer. The tracker handles race conditions
 * where a new topology update (a new rebalance) supersedes an ongoing state transfer, ensuring listeners are notified
 * appropriately (usually via cancellation or by waiting for the newer topology).
 * </p>
 *
 * @since 15.0
 */
@ThreadSafe
@Scope(Scopes.NAMED_CACHE)
public class StateTransferTracker {
   private static final Log log = LogFactory.getLog(StateTransferTracker.class);

   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject
   protected String cacheName;

   private final ReentrantLock lock = new ReentrantLock();

   @GuardedBy("lock")
   private final FutureHandler<Void> consumer = new FutureHandler<>();

   @GuardedBy("lock")
   private final FutureHandler<Void> provider = new FutureHandler<>();

   @GuardedBy("lock")
   private final FutureHandler<CacheTopology> stable = new FutureHandler<>();

   @GuardedBy("lock")
   private CacheTopology currentTopology = null;

   private final List<Entry> listeners = new CopyOnWriteArrayList<>();

   /**
    * Checks if a state transfer is currently in progress.
    *
    * <p>
    * A state transfer is considered "in progress" if any of the three tracking components (Consumer, Provider, or Stable
    * Topology) are pending completion for the current topology generation.
    * </p>
    *
    * @return {@code true} if state transfer is active; {@code false} otherwise.
    */
   public boolean isStateTransferInProgress() {
      lock.lock();
      try {
         return stable.isPending();
      } finally {
         lock.unlock();
      }
   }

   /**
    * Registers a listener to be notified when the state transfer completes.
    *
    * <p>
    * The listener is a {@link BiPredicate} that accepts:
    * <ul>
    * <li>{@link CacheTopology}: The stable topology that marked the completion of the transfer (or null if failed).</li>
    * <li>{@link Throwable}: An exception if the transfer failed or was cancelled (e.g., superseded by a newer topology).</li>
    * </ul>
    * </p>
    *
    * <p>
    * The listener must return a {@link Boolean}:
    * <ul>
    * <li>{@code true}: To indicate the listener has handled the event and should be removed. The returned {@link CompletionStage}
    *                   will complete successfully.</li>
    * <li>{@code false}: To indicate the listener wishes to stay registered (e.g., waiting for a future topology).
    *                   The returned {@link CompletionStage} will NOT complete yet.</li>
    * </ul>
    * </p>
    *
    * @param listener The function to invoke upon completion.
    * @return A {@link CompletionStage} that completes when the listener returns {@code true}.
    */
   public CompletionStage<Void> onStateTransferCompleted(BiPredicate<CacheTopology, Throwable> listener) {
      lock.lock();
      try {
         if (log.isTraceEnabled())
            log.tracef("waiting state completion %s (consumer=%s) (provider=%s)%n", cacheName, consumer.isPending(), provider.isPending());

         // If transfer is already done, try to invoke the listener immediately.
         if (!isStateTransferInProgress()
               && currentTopology != null
               && listener.test(currentTopology, null)) {
               return CompletableFutures.completedNull();
         }

         // Otherwise, queue it up.
         // Caller is waiting to complete in a future state transfer.
         CompletableFuture<Void> cf = new CompletableFuture<>();
         listeners.add(new Entry(cf, listener));
         return cf;
      } finally {
         lock.unlock();
      }
   }

   /**
    * Notifies the tracker of a topology update.
    *
    * <p>
    * If the provided topology is stable (has no pending consistent hash), this method marks the "Stable" phase as complete.
    * If all other phases are also done, it triggers the notification of waiting listeners.
    * </p>
    *
    * @param cacheTopology The new cache topology.
    */
   public void cacheTopologyUpdated(CacheTopology cacheTopology) {
      boolean isStableTopology = cacheTopology.getPendingCH() == null;
      if (isStableTopology) {
         lock.lock();
         try {
            log.tracef("Installed stable topology for %s with %s, state transfer is done now", cacheName, cacheTopology);
            int previousTopologyId = currentTopology != null ? currentTopology.getTopologyId() : Integer.MIN_VALUE;
            this.currentTopology = cacheTopology;

            if (!isStateTransferInProgress()) {
               notifyListeners(cacheTopology, null);
               return;
            }

            // Utilize a negative value to bypass ID verification inside the handler.
            stable.complete(Integer.MIN_VALUE, cacheTopology);

            // Ensure previous phases are marked complete if we jumped to a stable state.
            // The IDs are utilized as previous and current in purpose.
            // A node only consume before a stable topology, you still provide with the stable topology.
            consumer.complete(previousTopologyId, null);
            provider.complete(currentTopology.getTopologyId(), null);
         } finally {
            lock.unlock();
         }
      }
   }

   /**
    * Signals the start of the state consumer for a specific topology ID.
    *
    * <p>
    * If the {@code topologyId} is higher than the currently tracked ID, the tracker resets all phases to track this new
    * generation, effectively canceling any previous pending state transfers.
    * </p>
    *
    * @param topologyId The topology ID associated with this state transfer start.
    */
   public void startStateConsumer(int topologyId) {
      lock.lock();
      try {
         log.tracef("starting state consumer %s", cacheName);
         checkTopologyId(topologyId);
      } finally {
         lock.unlock();
      }
   }

   /**
    * Signals the completion of the state consumer for a specific topology ID.
    *
    * @param topologyId The topology ID that completed.
    */
   public void completeStateConsumer(int topologyId) {
      lock.lock();
      try {
         log.tracef("stopping state consumer %s", cacheName);
         consumer.complete(topologyId, null);
      } finally {
        lock.unlock();
      }
   }

   /**
    * Signals the start of the state provider tasks for a specific topology ID.
    *
    * <p>
    * Similar to {@link #startStateConsumer(int)}, this ensures the tracker is synchronized with the given
    * topology ID.
    * </p>
    *
    * @param topologyId The topology ID associated with this state transfer start.
    */
   public void startStateProvider(int topologyId) {
      lock.lock();
      try {
         log.tracef("starting state provider %s", cacheName);
         checkTopologyId(topologyId);
      } finally {
         lock.unlock();
      }
   }

   /**
    * Signals the completion of the State Provider phase for a specific topology ID.
    *
    * @param topologyId The topology ID that completed.
    */
   public void completeStateProvider(int topologyId) {
      lock.lock();
      try {
         log.tracef("stopping state provider %s", cacheName);
         provider.complete(topologyId, null);
      } finally {
         lock.unlock();
      }
   }

   /**
    * Internal check to handle topology generation changes.
    *
    * <p>
    * If the incoming {@code topologyId} represents a new generation, this method resets the internal futures, creates an
    * aggregate {@link CompletableFuture}, and attaches a callback to notify listeners when all phases (Consumer, Provider, Stable)
    * for this generation are complete.
    * </p>
    *
    * @param topologyId The new topology ID to check.
    */
   @GuardedBy("lock")
   private void checkTopologyId(int topologyId) {
      if (currentTopology == null || topologyId > currentTopology.getTopologyId()) {
         consumer.reset(topologyId);
         provider.reset(topologyId);
         stable.reset(topologyId);

         // Utilize local variables to avoid locking in the callback.
         CompletableFuture<CacheTopology> stableCF = stable.future();
         CompletableFuture<Void> providerCF = provider.future();
         CompletableFuture<Void> consumerCF = consumer.future();

         CompletableFuture.allOf(consumerCF, providerCF, stableCF)
               .whenComplete((ignore, t) -> {
                  if (CompletableFutures.extractException(t) instanceof CancellationException) {
                     return;
                  }

                  if (t != null) {
                     notifyListeners(null, t);
                     return;
                  }

                  assert stableCF.isDone() : "Stable topology was not done!";
                  notifyListeners(stableCF.join(), null);
               });
      }
   }

   private void notifyListeners(CacheTopology topology, Throwable t) {
      log.tracef("notifying all listeners topology installed: %s (%s)", topology, t);
      listeners.removeIf(e -> e.invokeListener(topology, t));
   }

   record Entry(CompletableFuture<Void> future, BiPredicate<CacheTopology, Throwable> listener) {

      /**
       * Invokes the listener and manages the completion of the associated future.
       *
       * @param ct The topology (or null on error).
       * @param t The exception (or null on success).
       * @return {@code true} if the listener handled the event and should be removed; {@code false} otherwise.
       */
      public boolean invokeListener(CacheTopology ct, Throwable t) {
         try {
            boolean v = listener.test(ct, t);
            if (v) future.complete(null);
            return v;
         } catch (Throwable t2) {
            log.errorf(t2, "Failed handling state transfer listener, discarded");
            future.completeExceptionally(t2);
            return true;
         }
      }
   }

   /**
    * A helper to track the completion of a single phase (Consumer, Provider, or Stable) tied to a specific topology ID.
    *
    * @param <T> The result type of the future.
    */
   private static final class FutureHandler<T> {
      private int id = Integer.MIN_VALUE;
      private CompletableFuture<T> future = CompletableFutures.completedNull();

      public boolean isPending() {
         return !future.isDone();
      }

      public void complete(int id, T t) {
         if (!isPending())
            return;

         if (id > 0 && this.id != id) {
            future.completeExceptionally(new CancellationException(String.format("Complete id (%d) does not match creation (%d)", id, this.id)));
            return;
         }

         future.complete(t);
      }

      public void reset(int id) {
         if (id == this.id)
            return;

         if (isPending()) {
            future.completeExceptionally(new CancellationException("A new state transfer started"));
         }
         this.id = id;
         future = new CompletableFuture<>();
      }

      public int id() {
         return id;
      }

      public CompletableFuture<T> future() {
         return future;
      }

      @Override
      public String toString() {
         return "FutureHandler{id=" + id + ", future=" + future + "}";
      }
   }
}
