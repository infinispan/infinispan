package org.infinispan.partitionhandling.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link PartitionHandlingManager} implementation when the cluster is always available.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class AvailablePartitionHandlingManager implements PartitionHandlingManager {


   private AvailablePartitionHandlingManager() {
   }

   public static AvailablePartitionHandlingManager getInstance() {
      return SingletonHolder.INSTANCE;
   }

   @Override
   public AvailabilityMode getAvailabilityMode() {
      return AvailabilityMode.AVAILABLE;
   }

   @Override
   public void setAvailabilityMode(AvailabilityMode availabilityMode) {/*no-op*/}

   @Override
   public void checkWrite(Object key) {/*no-op*/}

   @Override
   public void checkRead(Object key) {/*no-op*/}

   @Override
   public void checkClear() {/*no-op*/}

   @Override
   public void checkBulkRead() {/*no-op*/}

   @Override
   public CacheTopology getLastStableTopology() {
      return null;
   }

   @Override
   public boolean addPartialRollbackTransaction(GlobalTransaction globalTransaction, Collection<Address> affectedNodes,
                                                Collection<Object> lockedKeys) {
      return false;
   }

   @Override
   public boolean addPartialCommit2PCTransaction(GlobalTransaction globalTransaction, Collection<Address> affectedNodes,
                                                 Collection<Object> lockedKeys, EntryVersionsMap newVersions) {
      return false;
   }

   @Override
   public boolean addPartialCommit1PCTransaction(GlobalTransaction globalTransaction, Collection<Address> affectedNodes,
                                                 Collection<Object> lockedKeys, List<WriteCommand> modifications) {
      return false;
   }

   @Override
   public boolean isTransactionPartiallyCommitted(GlobalTransaction globalTransaction) {
      return false;
   }

   @Override
   public Collection<GlobalTransaction> getPartialTransactions() {
      return Collections.emptyList();
   }

   @Override
   public boolean canRollbackTransactionAfterOriginatorLeave(GlobalTransaction globalTransaction) {
      return true;
   }

   @Override
   public void onTopologyUpdate(CacheTopology cacheTopology) {/*no-op*/}

   private static class SingletonHolder {
      private static final AvailablePartitionHandlingManager INSTANCE = new AvailablePartitionHandlingManager();
   }
}
