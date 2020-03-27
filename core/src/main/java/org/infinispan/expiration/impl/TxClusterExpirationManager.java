package org.infinispan.expiration.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.transaction.LockingMode;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class TxClusterExpirationManager<K, V> extends ClusterExpirationManager<K, V> {

   private boolean optimisticTransaction;

   @Override
   public void start() {
      super.start();

      optimisticTransaction = configuration.transaction().lockingMode() == LockingMode.OPTIMISTIC;
   }

   @Override
   boolean waitOnLifespanExpiration(boolean isWrite) {
      // We always have to wait when using optimistic transactions when an entry was found to be expired.
      // An example is a user calling get then put if the value was null.
      // This can cause an expiration event to be fired concurrently with the put.
      // In an optimistic transaction this can cause an issue as the put may get a write skew exception.
      // By waiting we prevent this case which can be confusing getting a write skew with a single requestor.
      return isWrite || optimisticTransaction;
   }

   @Override
   AdvancedCache<K, V> cacheToUse(boolean isWrite) {
      // For non writes we don't have any lock wait time. This is to prevent deadlocks with a concurrent write and
      // also so that with concurrent gets, only one will win.
      // Pessimistic caches will already hold the lock for the read value and thus we have to skip locking
      // Optimistic will not hold the lock at this point and thus we always have to lock the entry
      return isWrite ? (optimisticTransaction ? cache : cache.withFlags(Flag.SKIP_LOCKING)) :
            cache.withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
   }
}
