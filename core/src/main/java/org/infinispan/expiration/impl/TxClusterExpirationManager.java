package org.infinispan.expiration.impl;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.transaction.LockingMode;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class TxClusterExpirationManager<K, V> extends ClusterExpirationManager<K, V> {

   @Inject
   protected KeyPartitioner partitioner;
   @Inject @ComponentName(KnownComponentNames.PERSISTENCE_EXECUTOR)
   protected ExecutorService blockingExecutor;

   private boolean optimisticTransaction;

   @Override
   public void start() {
      super.start();

      optimisticTransaction = configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC;
   }

   @Override
   CompletableFuture<Boolean> handleMaxIdleExpireEntry(K key, V value, long maxIdle, boolean skipLocking) {
      // We cannot remove an entry in a tx, due to possible dead lock - thus we just return
      // if the entry is expired or not
      return retrieveLastAccess(key, value, partitioner.getSegment(key)).thenApply(Objects::isNull);
   }

   @Override
   CompletableFuture<Void> removeLifespan(AdvancedCache<K, V> cacheToUse, K key, V value, long lifespan) {
      // Transactional is still blocking - to be removed later
      return CompletableFuture.supplyAsync(() -> super.removeLifespan(cacheToUse, key, value, lifespan), blockingExecutor)
            .thenCompose(Function.identity());
   }

   @Override
   CompletableFuture<Boolean> removeMaxIdle(AdvancedCache<K, V> cacheToUse, K key, V value) {
      // Transactional is still blocking - to be removed later
      return CompletableFuture.supplyAsync(() -> cacheToUse.removeMaxIdleExpired(key, value), blockingExecutor)
                  .thenCompose(Function.identity());
   }

   @Override
   boolean waitOnLifespanExpiration(boolean hasLock) {
      // We always have to wait when using optimistic transactions. A scenario where a single writer may call get
      // and then put may get write skew problems otherwise. The problem is that the get when it sees the expired
      // entry will fire the remove expired. If we don't wait on this we may end up running the put command at the
      // same time and get a write skew issue in either caller, which would be very confusing when there is only a
      // single user of the cache
      return hasLock || optimisticTransaction;
   }
}
