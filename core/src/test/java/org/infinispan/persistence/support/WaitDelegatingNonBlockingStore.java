package org.infinispan.persistence.support;

import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.persistence.spi.NonBlockingStore;

public class WaitDelegatingNonBlockingStore<K, V> extends DelegatingNonBlockingStore<K, V> implements WaitNonBlockingStore<K, V> {
   private final NonBlockingStore<K, V> nonBlockingStore;
   private final KeyPartitioner keyPartitioner;

   public WaitDelegatingNonBlockingStore(NonBlockingStore<K, V> nonBlockingStore, KeyPartitioner keyPartitioner) {
      this.nonBlockingStore = nonBlockingStore;
      this.keyPartitioner = keyPartitioner;
   }

   @Override
   public NonBlockingStore<K, V> delegate() {
      return nonBlockingStore;
   }

   @Override
   public KeyPartitioner getKeyPartitioner() {
      return keyPartitioner;
   }
}
