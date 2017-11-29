package org.infinispan.hibernate.cache.commons.access;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

public class UnorderedReplicationLogic extends ClusteringDependentLogic.ReplicationLogic {

   @Override
   public Commit commitType(
         FlagAffectedCommand command, InvocationContext ctx, Object key, boolean removed) {
      Commit commit = super.commitType( command, ctx, key, removed );
      return commit == Commit.NO_COMMIT ? Commit.COMMIT_LOCAL : commit;
   }

}
