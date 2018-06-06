package org.infinispan.test.fwk;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.remoting.transport.Address;

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
   public LocalizedCacheTopology getCacheTopology() {
      return clusteringDependentLogic.getCacheTopology();
   }

   @Override
   public void commitEntry(CacheEntry entry, FlagAffectedCommand command, InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
      clusteringDependentLogic.commitEntry(entry, command, ctx, trackFlag, l1Invalidation);
   }

   @Override
   public Commit commitType(FlagAffectedCommand command, InvocationContext ctx, int segment, boolean removed) {
      return clusteringDependentLogic.commitType(command, ctx, segment, removed);
   }

   @Override
   public EntryVersionsMap createNewVersionsAndCheckForWriteSkews(VersionGenerator versionGenerator, TxInvocationContext context, VersionedPrepareCommand prepareCommand) {
      return clusteringDependentLogic.createNewVersionsAndCheckForWriteSkews(versionGenerator, context, prepareCommand);
   }

   @Override
   public Address getAddress() {
      return clusteringDependentLogic.getAddress();
   }
}
