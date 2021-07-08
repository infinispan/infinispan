package org.infinispan.distribution.ch.impl;

import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.persistence.manager.PersistenceManager.StoreChangeListener;
import org.infinispan.persistence.manager.PersistenceStatus;

/**
 *
 * @since 13.0
 */
class KeyPartitionerDelegate implements KeyPartitioner, StoreChangeListener {

   private final KeyPartitioner keyPartitioner;
   private volatile boolean needSegments;

   public KeyPartitionerDelegate(KeyPartitioner keyPartitioner, Configuration configuration) {
      this.keyPartitioner = keyPartitioner;
      this.needSegments = Configurations.needSegments(configuration);
   }

   @Override
   public int getSegment(Object key) {
      return needSegments ? keyPartitioner.getSegment(key) : 0;
   }

   @Override
   public void storeChanged(PersistenceStatus persistenceStatus) {
      synchronized (this) {
         needSegments = needSegments || persistenceStatus.usingSegmentedStore();
      }
   }
}
