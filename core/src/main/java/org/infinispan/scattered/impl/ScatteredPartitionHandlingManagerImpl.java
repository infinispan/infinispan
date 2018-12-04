package org.infinispan.scattered.impl;

import java.util.List;

import org.infinispan.commons.util.EnumUtil;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.partitionhandling.impl.PartitionHandlingManagerImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class ScatteredPartitionHandlingManagerImpl extends PartitionHandlingManagerImpl {
   private static final Log log = LogFactory.getLog(ScatteredPartitionHandlingManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public void doCheck(Object key, boolean isWrite, long flagBitSet) {
      AvailabilityMode availabilityMode = getAvailabilityMode();
      if (trace) log.tracef("Checking availability for key=%s, status=%s", key, availabilityMode);
      if (availabilityMode == AvailabilityMode.AVAILABLE)
         return;

      LocalizedCacheTopology cacheTopology = distributionManager.getCacheTopology();
      if (isKeyOperationAllowed(isWrite, flagBitSet, cacheTopology, key))
         return;
      if (trace) log.tracef("Partition is in %s mode, access is not allowed for key %s", availabilityMode, key);
      throw log.degradedModeKeyUnavailable(key);
   }

   @Override
   protected boolean isKeyOperationAllowed(boolean isWrite, long flagBitSet, LocalizedCacheTopology cacheTopology,
                                           Object key) {
      // If the partition is degraded, we cannot allow writes even to the keys which we are primary owner
      // since the other partition may be available (thinking that we are dead) and it can also modify
      // the entry.
      // With ALLOW_READS we allow stale (possibly stale) reads if the primary owner is in the local partition,
      // with DENY_READ_WRITES we deny all reads.
      if (getAvailabilityMode() == AvailabilityMode.AVAILABLE)
         return true;

      List<Address> actualMembers = cacheTopology.getActualMembers();
      switch (getPartitionHandling()) {
         case ALLOW_READ_WRITES:
            return true;
         case ALLOW_READS:
            if (isWrite || EnumUtil.containsAny(flagBitSet, FlagBitSets.FORCE_WRITE_LOCK)) {
               // Writes and locks don't work in degraded mode
               return false;
            } else {
               // Reads only require the primary owner in the local partition
               return actualMembers.contains(cacheTopology.getDistribution(key).primary());
            }
         default:
            // Neither reads nor writes work in DENY_READ_WRITES degraded mode
            // since the other partition may be available (thinking that we are dead) and it may modify the entry.
            return false;
      }
   }
}
