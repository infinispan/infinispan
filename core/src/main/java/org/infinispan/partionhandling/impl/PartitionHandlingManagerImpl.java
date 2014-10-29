package org.infinispan.partionhandling.impl;

import org.infinispan.Cache;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.partionhandling.AvailabilityMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
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
   private RpcManager rpcManager;
   private LocalTopologyManager localTopologyManager;
   private String cacheName;
   private CacheNotifier notifier;

   @Inject void init(DistributionManager distributionManager, RpcManager rpcManager,
         LocalTopologyManager localTopologyManager, Cache cache, CacheNotifier notifier) {
      this.distributionManager = distributionManager;
      this.rpcManager = rpcManager;
      this.localTopologyManager = localTopologyManager;
      this.cacheName = cache.getName();
      this.notifier = notifier;
   }

   @Start void start() {
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
      // TODO The JGroups view is updated before the cache topology, so it's possible to access a stale key
      // just after a merge (if the other partition was AVAILABLE).
      if (!rpcManager.getTransport().getMembers().containsAll(owners)) {
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
   public CacheTopology getLastStableTopology() {
      return localTopologyManager.getStableCacheTopology(cacheName);
   }
}
