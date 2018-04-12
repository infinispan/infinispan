package org.infinispan.expiration.impl;

import java.util.concurrent.CompletableFuture;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class TxClusterExpirationManager<K, V> extends ClusterExpirationManager<K, V> {
   @Override
   CompletableFuture<Boolean> handleMaxIdleExpireEntry(K key, V value, long maxIdle) {
      // We cannot remove an entry in a tx, due to possible dead lock - thus we just return
      // if the entry is expired or not
      return retrieveLastAccess(key, value).thenApply(hasMaxIdle -> hasMaxIdle == null);
   }
}
