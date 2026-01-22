package org.infinispan.statetransfer;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.annotations.GuardedBy;

import com.google.errorprone.annotations.ThreadSafe;

@ThreadSafe
@Scope(Scopes.NAMED_CACHE)
public class StateTransferTracker {
   private static final Log log = LogFactory.getLog(StateTransferTracker.class);
   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject
   protected String cacheName;
   @Inject protected ComponentRef<RpcManager> rpcManager;

   private final ReentrantLock lock = new ReentrantLock();

   @GuardedBy("lock")
   private final FutureHandler<Void> consumer = new FutureHandler<>();

   @GuardedBy("lock")
   private final FutureHandler<Void> provider = new FutureHandler<>();

   @GuardedBy("lock")
   private final FutureHandler<CacheTopology> stable = new FutureHandler<>();

   @GuardedBy("lock")
   private CacheTopology currentTopology = null;

   private final List<BiFunction<CacheTopology, Throwable, Boolean>> listeners = new CopyOnWriteArrayList<>();

   private String address;

   protected void start() {
      address = rpcManager.running().getAddress().toString();
   }

   public boolean isStateTransferInProgress() {
      lock.lock();
      try {
         return consumer.isPending() || provider.isPending() || stable.isPending();
      } finally {
         lock.unlock();
      }
   }

   public void onStateTransferCompleted(BiFunction<CacheTopology, Throwable, Boolean> listener) {
      lock.lock();
      try {
         log.tracef("%s: waiting state completion %s (consumer=%s) (provider=%s)%n", address, cacheName, consumer.isPending(), provider.isPending());
         if (!isStateTransferInProgress()
               && currentTopology != null
               && invokeListener(currentTopology, null, listener)) {
               return;
         }

         listeners.add(listener);
      } finally {
         lock.unlock();
      }
   }

   public void cacheTopologyUpdated(CacheTopology cacheTopology) {
      boolean isStableTopology = cacheTopology.getPendingCH() == null;
      if (isStableTopology) {
         log.tracef("Installed stable topology for %s with %s, state transfer is done now", cacheName, cacheTopology);
         int previousTopologyId = currentTopology != null ? currentTopology.getTopologyId() : Integer.MIN_VALUE;
         this.currentTopology = cacheTopology;

         if (!isStateTransferInProgress()) {
            notifyListeners(cacheTopology, null);
            return;
         }

         // Utilize a negative value to bypass ID verification.
         stable.complete(Integer.MIN_VALUE, cacheTopology);
         consumer.complete(previousTopologyId, null);
         provider.complete(currentTopology.getTopologyId(), null);
      }
   }

   public void startStateConsumer(int topologyId) {
      lock.lock();
      try {
         log.tracef("%s: starting state consumer %s", address, cacheName);
         checkTopologyId(topologyId);
      } finally {
         lock.unlock();
      }
   }

   public void completeStateConsumer(int topologyId) {
      lock.lock();
      try {
         log.tracef("%s: stopping state consumer %s", address, cacheName);
         consumer.complete(topologyId, null);
      } finally {
        lock.unlock();
      }
   }

   public void startStateProvider(int topologyId) {
      lock.lock();
      try {
         log.tracef("%s: starting state provider %s", address, cacheName);
         checkTopologyId(topologyId);
      } finally {
         lock.unlock();
      }
   }

   public void completeStateProvider(int topologyId) {
      lock.lock();
      try {
         log.tracef("%s: stopping state provider %s", address, cacheName);
         provider.complete(topologyId, null);
      } finally {
         lock.unlock();
      }
   }

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
      listeners.removeIf(bf -> invokeListener(topology, t, bf));
   }

   private boolean invokeListener(CacheTopology ct, Throwable t, BiFunction<CacheTopology, Throwable, Boolean> listener) {
      try {
         return listener.apply(ct, t);
      } catch (Throwable t2) {
         log.errorf(t2, "Failed handling state transfer listener of %s, discarded", cacheName);
         return true;
      }
   }

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
