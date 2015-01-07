package org.infinispan.partitionhandling.impl;

import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;

public class PartitionHandlingManagerImpl implements PartitionHandlingManager {
   private static final Log log = LogFactory.getLog(PartitionHandlingManagerImpl.class);
   private static final boolean trace = log.isTraceEnabled();

   private volatile AvailabilityMode availabilityMode = AvailabilityMode.AVAILABLE;

   private DistributionManager distributionManager;
   private LocalTopologyManager localTopologyManager;
   private StateTransferManager stateTransferManager;
   private String cacheName;
   private CacheNotifier notifier;

   @Inject
   void init(DistributionManager distributionManager, LocalTopologyManager localTopologyManager,
         StateTransferManager stateTransferManager, Cache cache, CacheNotifier notifier) {
      this.distributionManager = distributionManager;
      this.localTopologyManager = localTopologyManager;
      this.stateTransferManager = stateTransferManager;
      this.cacheName = cache.getName();
      this.notifier = notifier;
   }

   @Start
   void start() {
   }

   @Override
   public void setAvailabilityMode(AvailabilityMode availabilityMode) {
      if (availabilityMode != this.availabilityMode) {
         log.debugf("Updating availability for cache %s: %s -> %s", cacheName, this.availabilityMode, availabilityMode);
         notifier.notifyPartitionStatusChanged(availabilityMode, true);
         this.availabilityMode = availabilityMode;
         notifier.notifyPartitionStatusChanged(availabilityMode, false);
      }
   }

   @Override
   public AvailabilityMode getAvailabilityMode() {
      return availabilityMode;
   }

   @Override
   public void checkWrite(Object key) {
      doCheck(key);
   }

   @Override
   public void checkRead(Object key) {
      doCheck(key);
   }

   private void doCheck(Object key) {
      if (trace) log.tracef("Checking availability for key=%s, status=%s", key, availabilityMode);
      if (availabilityMode == AvailabilityMode.AVAILABLE)
         return;

      List<Address> owners = distributionManager.locate(key);
      List<Address> actualMembers = stateTransferManager.getCacheTopology().getActualMembers();
      if (!actualMembers.containsAll(owners)) {
         if (trace) log.tracef("Partition is in %s mode, access is not allowed for key %s", availabilityMode, key);
         throw log.degradedModeKeyUnavailable(key);
      } else {
         if (trace) log.tracef("Key %s is available.", key);
      }
   }

   @Override
   public void checkClear() {
      if (availabilityMode != AvailabilityMode.AVAILABLE) {
         throw log.clearDisallowedWhilePartitioned();
      }
   }

   @Override
   public void checkBulkRead() {
      if (availabilityMode != AvailabilityMode.AVAILABLE) {
         throw log.partitionDegraded();
      }
   }

   @Override
   public CacheTopology getLastStableTopology() {
      return localTopologyManager.getStableCacheTopology(cacheName);
   }
}
