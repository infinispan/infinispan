package org.infinispan.scattered.impl;

import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManagerImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class ScatteredPartitionHandlingManagerImpl extends PartitionHandlingManagerImpl {
   private static final Log log = LogFactory.getLog(ScatteredPartitionHandlingManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public void doCheck(Object key) {
      AvailabilityMode availabilityMode = getAvailabilityMode();
      if (trace) log.tracef("Checking availability for key=%s, status=%s", key, availabilityMode);
      if (availabilityMode == AvailabilityMode.AVAILABLE)
         return;
      // If the partition is degraded, we cannot allow writes even to the keys which we are primary owner
      // since the other partition may be available (thinking that we are dead) and it can also modify
      // the entry. We cannot allow reads either as they can be modified in the other partition and
      // we would return stale values.
      if (trace) log.tracef("Partition is in %s mode, access is not allowed for key %s", availabilityMode, key);
      throw log.degradedModeKeyUnavailable(key);
   }
}
