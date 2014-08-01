package org.infinispan.partionhandling.impl;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.partionhandling.AvailabilityException;
import org.infinispan.partionhandling.PartitionHandlingStrategy;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.ClusterCacheStatus;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.List;

public class PartitionHandlingManager {

   public enum PartitionState {
      AVAILABLE, UNAVAILABLE, DEGRADED_MODE
   }

   private volatile List<Address> lastStableCluster = Collections.emptyList();

   private static Log log = LogFactory.getLog(PartitionHandlingManager.class);

   private Cache cache;

   private PartitionHandlingStrategy partitionHandlingStrategy;

   private PartitionState state = PartitionState.AVAILABLE;

   private DistributionManager distributionManager;
   private RpcManager rpcManager;

   @Inject void init(Cache cache, DistributionManager distributionManager, RpcManager rpcManager) {
      this.cache = cache;
      this.distributionManager = distributionManager;
      this.rpcManager = rpcManager;
   }

   @Start void start() {
      partitionHandlingStrategy = new DegradedModePartitionHandlingStrategy();
   }

   public void setState(PartitionState state) {
      log.tracef("Updating partition state: %s -> %s", this.state, state);
      this.state = state;
   }

   public void setLastStableCluster(List<Address> members) {
      lastStableCluster = members;
   }

   public List<Address> getLastStableCluster() {
      return lastStableCluster;
   }

   public PartitionState getState() {
      return state;
   }

   public boolean handleViewChange(List<Address> newMembers, ClusterCacheStatus topologyManager) {
      boolean missingData = isMissingData(newMembers, lastStableCluster);
      log.tracef("handleViewChange(old:%s -> new:%s). Is missing data? %s", lastStableCluster, newMembers, missingData);
      PartitionContextImpl pci = new PartitionContextImpl(this, lastStableCluster, newMembers, missingData, topologyManager, cache);
      log.debugf("Invoking partition handling %s", pci);
      partitionHandlingStrategy.onMembershipChanged(pci);
      return pci.isRebalance();
   }

   private boolean isMissingData(List<Address> newMembers, List<Address> oldMembers) {
      //todo - we can be better than this, e.g. take into account topology information
      int missingMembers = 0;
      for (Address a : oldMembers)
         if (!newMembers.contains(a)) missingMembers++;
      Configuration cacheConfiguration = cache.getCacheConfiguration();
      int numOwners = cacheConfiguration.clustering().hash().numOwners();
      return  missingMembers >= numOwners;
   }

   public void enterDegradedMode() {
      log.debug("Entering in degraded mode.");
      if (state == PartitionState.DEGRADED_MODE)
         throw new IllegalStateException("Already in degraded mode!");
      setState(PartitionState.DEGRADED_MODE);
      PartitionStateControlCommand stateUpdateCommand = new PartitionStateControlCommand(cache.getName(), PartitionState.DEGRADED_MODE);
      rpcManager.invokeRemotely(null, stateUpdateCommand, rpcManager.getDefaultRpcOptions(true));
   }

   public void checkWrite(Object key) {
      log.tracef("Check write for key=%s, status=%s", key, state);
      if (state == PartitionState.AVAILABLE) return;
      if (state == PartitionState.UNAVAILABLE)
         throw new AvailabilityException("Cluster is UNAVAILABLE because of node failures.");
      List<Address> owners = distributionManager.locate(key);
      if (! rpcManager.getTransport().getMembers().containsAll(owners)) {
         log.tracef("Partition is in %s mode, access is not allowed for key %s", state, key);
         throw new AvailabilityException("Not all owners of key '" + key + "' are in this partition");
      } else {
         log.tracef("Key %s is writable.", key);
      }
   }

   public void checkRead(Object key) {
      checkWrite(key);
   }

   public void checkClear() {
      if (state == PartitionState.DEGRADED_MODE)
         throw new AvailabilityException("Cannot clear when the cluster is partitioned");
   }

}
