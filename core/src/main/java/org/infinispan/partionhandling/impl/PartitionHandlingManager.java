package org.infinispan.partionhandling.impl;

import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.topology.CacheTopology;

/**
 * @author Dan Berindei
 * @since 7.0
 */
public interface PartitionHandlingManager {
   void setAvailabilityMode(AvailabilityMode availabilityMode);

   AvailabilityMode getAvailabilityMode();

   void checkWrite(Object key);

   void checkRead(Object key);

   void checkClear();

   CacheTopology getLastStableTopology();
}
