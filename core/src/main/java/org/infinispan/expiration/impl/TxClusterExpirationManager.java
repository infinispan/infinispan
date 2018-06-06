package org.infinispan.expiration.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.annotations.Inject;

import net.jcip.annotations.ThreadSafe;

@ThreadSafe
public class TxClusterExpirationManager<K, V> extends ClusterExpirationManager<K, V> {

   @Inject
   protected KeyPartitioner partitioner;

   @Override
   CompletableFuture<Boolean> handleMaxIdleExpireEntry(K key, V value, long maxIdle) {
      // We cannot remove an entry in a tx, due to possible dead lock - thus we just return
      // if the entry is expired or not
      return retrieveLastAccess(key, value, partitioner.getSegment(key)).thenApply(hasMaxIdle -> hasMaxIdle == null);
   }
}
