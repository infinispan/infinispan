package org.infinispan.partionhandling.impl;

import org.infinispan.Cache;
import org.infinispan.partionhandling.PartitionContext;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.ClusterCacheStatus;

import java.util.List;

public class PartitionContextImpl implements PartitionContext {
   private final PartitionHandlingManager partitionHandlingManager;
   private final boolean isMissingData;
   private final List<Address> oldMembers;
   private final List<Address> newMembers;
   private final ClusterCacheStatus clusterCacheStatus;
   private boolean rebalance;
   private final Cache cache;

   public PartitionContextImpl(PartitionHandlingManager partitionHandlingManager, List<Address> oldMembers, List<Address> newMembers, boolean isMissingData, ClusterCacheStatus clusterCacheStatus, Cache c) {
      this.partitionHandlingManager = partitionHandlingManager;
      this.oldMembers = oldMembers;
      this.newMembers = newMembers;
      this.isMissingData = isMissingData;
      this.clusterCacheStatus = clusterCacheStatus;
      this.cache = c;
   }

   @Override
   public List<Address> getOldMembers() {
      return oldMembers;
   }

   @Override
   public List<Address> getNewMembers() {
      return newMembers;
   }

   @Override
   public boolean isDataLost() {
      return isMissingData;
   }

   @Override
   public void currentPartitionDegradedMode() {
      partitionHandlingManager.enterDegradedMode();
   }

   @Override
   public void rebalance() {
      this.rebalance = true;
   }

   public boolean isRebalance() {
      return rebalance;
   }

   @Override
   public Cache getCache() {
      return cache;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PartitionContextImpl that = (PartitionContextImpl) o;

      if (isMissingData != that.isMissingData) return false;
      if (rebalance != that.rebalance) return false;
      if (clusterCacheStatus != null ? !clusterCacheStatus.equals(that.clusterCacheStatus) : that.clusterCacheStatus != null)
         return false;
      if (newMembers != null ? !newMembers.equals(that.newMembers) : that.newMembers != null) return false;
      if (oldMembers != null ? !oldMembers.equals(that.oldMembers) : that.oldMembers != null) return false;
      if (partitionHandlingManager != null ? !partitionHandlingManager.equals(that.partitionHandlingManager) : that.partitionHandlingManager != null)
         return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = partitionHandlingManager != null ? partitionHandlingManager.hashCode() : 0;
      result = 31 * result + (isMissingData ? 1 : 0);
      result = 31 * result + (oldMembers != null ? oldMembers.hashCode() : 0);
      result = 31 * result + (newMembers != null ? newMembers.hashCode() : 0);
      result = 31 * result + (clusterCacheStatus != null ? clusterCacheStatus.hashCode() : 0);
      result = 31 * result + (rebalance ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      return "PartitionContextImpl{" +
            "partitionHandlingManager=" + partitionHandlingManager +
            ", isMissingData=" + isMissingData +
            ", oldMembers=" + oldMembers +
            ", newMembers=" + newMembers +
            ", clusterCacheStatus=" + clusterCacheStatus +
            ", rebalance=" + rebalance +
            '}';
   }
}
