package org.infinispan.test.fwk;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.List;

/**
 * A {@link org.infinispan.interceptors.locking.ClusteringDependentLogic} delegator
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
public class ClusteringDependentLogicDelegator implements ClusteringDependentLogic {

   private final ClusteringDependentLogic clusteringDependentLogic;

   public ClusteringDependentLogicDelegator(ClusteringDependentLogic clusteringDependentLogic) {
      this.clusteringDependentLogic = clusteringDependentLogic;
   }

   @Override
   public boolean localNodeIsOwner(Object key) {
      return clusteringDependentLogic.localNodeIsOwner(key);
   }

   @Override
   public boolean localNodeIsPrimaryOwner(Object key) {
      return clusteringDependentLogic.localNodeIsPrimaryOwner(key);
   }

   @Override
   public Address getPrimaryOwner(Object key) {
      return clusteringDependentLogic.getPrimaryOwner(key);
   }

   @Override
   public void commitEntry(CacheEntry entry, Metadata metadata, FlagAffectedCommand command, InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
      clusteringDependentLogic.commitEntry(entry, metadata, command, ctx, trackFlag, l1Invalidation);
   }

   @Override
   public List<Address> getOwners(Collection<Object> keys) {
      return clusteringDependentLogic.getOwners(keys);
   }

   @Override
   public List<Address> getOwners(Object key) {
      return clusteringDependentLogic.getOwners(key);
   }

   @Override
   public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
      return clusteringDependentLogic.createNewVersionsAndCheckForWriteSkews(versionGenerator, context, prepareCommand);
   }

   @Override
   public Address getAddress() {
      return clusteringDependentLogic.getAddress();
   }

   @Override
   public boolean lock(Object key, boolean noWaitTime) throws InterruptedException {
      return clusteringDependentLogic.lock(key, noWaitTime);
   }

   @Override
   public void unlock(Object key) {
      clusteringDependentLogic.unlock(key);
   }
}
