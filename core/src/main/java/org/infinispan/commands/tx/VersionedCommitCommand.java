package org.infinispan.commands.tx;

import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * The same as a {@link CommitCommand} except that version information is also carried by this command, used by
 * optimistically transactional caches making use of write skew checking when using {@link org.infinispan.util.concurrent.IsolationLevel#REPEATABLE_READ}.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedCommitCommand extends CommitCommand {
   public static final byte COMMAND_ID = 27;
   private EntryVersionsMap updatedVersions;

   public VersionedCommitCommand() {
      super("");
   }

   public VersionedCommitCommand(String cacheName, GlobalTransaction gtx) {
      super(cacheName, gtx);
   }

   public VersionedCommitCommand(String cacheName) {
      super(cacheName);
   }

   public EntryVersionsMap getUpdatedVersions() {
      return updatedVersions;
   }

   public void setUpdatedVersions(EntryVersionsMap updatedVersions) {
      this.updatedVersions = updatedVersions;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{globalTx, updatedVersions};
   }

   @Override
   public void setParameters(int commandId, Object[] args) {
      globalTx = (GlobalTransaction) args[0];
      updatedVersions = (EntryVersionsMap) args[1];
   }

   @Override
   public String toString() {
      return "VersionedCommitCommand{gtx=" + globalTx +
            ", cacheName='" + cacheName + '\'' +
            ", topologyId=" + getTopologyId() +
            ", updatedVersions=" + updatedVersions +
            '}';
   }
}
