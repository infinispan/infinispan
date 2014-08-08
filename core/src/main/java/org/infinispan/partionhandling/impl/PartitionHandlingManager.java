package org.infinispan.partionhandling.impl;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
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

   private static final Log log = LogFactory.getLog(PartitionHandlingManager.class);
   private static final boolean trace = log.isTraceEnabled();

   private volatile List<Address> lastStableCluster = Collections.emptyList();

   private Cache cache;

   private PartitionHandlingStrategy partitionHandlingStrategy;

   private PartitionState state = PartitionState.AVAILABLE;

   private DistributionManager distributionManager;
   private RpcManager rpcManager;
   private Configuration configuration;

   @Inject void init(Cache cache, DistributionManager distributionManager, RpcManager rpcManager, Configuration configuration) {
      this.cache = cache;
      this.distributionManager = distributionManager;
      this.rpcManager = rpcManager;
      this.configuration = configuration;
   }

   @Start void start() {
      partitionHandlingStrategy = new DegradedModePartitionHandlingStrategy();
   }

   public void setState(PartitionState state) {
      if (trace) log.tracef("Updating partition state: %s -> %s", this.state, state);
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
      if (trace) log.tracef("handleViewChange(old:%s -> new:%s). Is missing data? %s", lastStableCluster, newMembers, missingData);
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
      int numOwners = configuration.clustering().hash().numOwners();
      return  missingMembers >= numOwners;
   }

   public void enterDegradedMode() {
      log.debug("Entering in degraded mode.");
      if (state == PartitionState.DEGRADED_MODE)
         throw new IllegalStateException("Already in degraded mode!");
      // Don't do anything if we're already in unavailable mode
      if (state == PartitionState.UNAVAILABLE)
         return;
      setState(PartitionState.DEGRADED_MODE);
      PartitionStateControlCommand stateUpdateCommand = new PartitionStateControlCommand(cache.getName(), PartitionState.DEGRADED_MODE);
      rpcManager.invokeRemotely(null, stateUpdateCommand, rpcManager.getDefaultRpcOptions(true));
   }

   public void checkWrite(Object key) {
      doCheck(key);
   }

   public void checkRead(Object key) {
      doCheck(key);
   }

   private void doCheck(Object key) {
      if (trace) log.tracef("Checking availability for key=%s, status=%s", key, state);
      if (state == PartitionState.AVAILABLE) return;
      if (state == PartitionState.UNAVAILABLE)
         throw log.partitionUnavailable();
      List<Address> owners = distributionManager.locate(key);
      if (!rpcManager.getTransport().getMembers().containsAll(owners)) {
         if (trace) log.tracef("Partition is in %s mode, access is not allowed for key %s", state, key);
         throw log.degradedModeKeyUnavailable(key);
      } else {
         if (trace) log.tracef("Key %s is available.", key);
      }
   }

   public void checkClear() {
      if (state != PartitionState.AVAILABLE) {
         throw log.clearDisallowedWhilePartitioned();
      }
   }

}
