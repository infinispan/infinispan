package org.infinispan.transaction.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import jakarta.transaction.Transaction;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * Object that holds transaction's state on the node where it originated; as opposed to {@link RemoteTransaction}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.0
 */
public abstract class LocalTransaction extends AbstractCacheTransaction {

   private static final Log log = LogFactory.getLog(LocalTransaction.class);

   private Set<Address> remoteLockedNodes;

   private final Transaction transaction;

   private final boolean implicitTransaction;

   private volatile boolean isFromRemoteSite;

   private boolean prepareSent;

   @GuardedBy("this")
   private Map<Object, CompletionStage<IracMetadata>> iracMetadata;

   public LocalTransaction(Transaction transaction, GlobalTransaction tx, boolean implicitTransaction, int topologyId,
                           long txCreationTime) {
      super(tx, topologyId, txCreationTime);
      this.transaction = transaction;
      this.implicitTransaction = implicitTransaction;
   }

   public final void addModification(WriteCommand mod) {
      if (log.isTraceEnabled()) log.tracef("Adding modification %s. Mod list is %s", mod, modifications);
      modifications.append(mod);
   }

   public void locksAcquired(Collection<Address> nodes) {
      if (log.isTraceEnabled()) log.tracef("Adding remote locks on %s. Remote locks are %s", nodes, remoteLockedNodes);
      if (remoteLockedNodes == null)
         remoteLockedNodes = new HashSet<>(nodes);
      else
         remoteLockedNodes.addAll(nodes);
   }

   public Collection<Address> getRemoteLocksAcquired(){
      if (remoteLockedNodes == null) return Collections.emptySet();
      return remoteLockedNodes;
   }

   public void clearRemoteLocksAcquired() {
      if (remoteLockedNodes != null) remoteLockedNodes.clear();
   }

   public Transaction getTransaction() {
      return transaction;
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return lookedUpEntries == null ? Collections.emptyMap() : lookedUpEntries;
   }

   public boolean isImplicitTransaction() {
      return implicitTransaction;
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      checkIfRolledBack();
      if (lookedUpEntries == null)
         lookedUpEntries = new HashMap<>(4);

      lookedUpEntries.put(key, e);
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> entries) {
      checkIfRolledBack();
      if (lookedUpEntries == null) {
         lookedUpEntries = new HashMap<>(entries);
      } else {
         lookedUpEntries.putAll(entries);
      }
   }

   public boolean isReadOnly() {
      return modifications.isEmpty();
   }

   public abstract boolean isEnlisted();

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LocalTransaction that = (LocalTransaction) o;

      return tx.getId() == that.tx.getId();
   }

   @Override
   public int hashCode() {
      long id = tx.getId();
      return (int)(id ^ (id >>> 32));
   }

   @Override
   public String toString() {
      return "LocalTransaction{" +
            "remoteLockedNodes=" + remoteLockedNodes +
            ", isMarkedForRollback=" + isMarkedForRollback() +
            ", lockedKeys=" + getLockedKeys() +
            ", backupKeyLocks=" + getBackupLockedKeys() +
            ", topologyId=" + topologyId +
            ", stateTransferFlag=" + getStateTransferFlag() +
            "} " + super.toString();
   }

   public void setStateTransferFlag(Flag stateTransferFlag) {
      if (this.getStateTransferFlag() == null &&
            (stateTransferFlag == Flag.PUT_FOR_STATE_TRANSFER ||
                   stateTransferFlag == Flag.PUT_FOR_X_SITE_STATE_TRANSFER)) {
         internalSetStateTransferFlag(stateTransferFlag);
      }
   }

   /**
    * When x-site replication is used, this returns when this operation
    * happens as a result of backing up data from a remote site.
    */
   public boolean isFromRemoteSite() {
      return isFromRemoteSite;
   }

   /**
    * @see #isFromRemoteSite()
    */
   public void setFromRemoteSite(boolean fromRemoteSite) {
      isFromRemoteSite = fromRemoteSite;
   }

   /**
    * Calculates the list of nodes to which a commit/rollback needs to be sent based on the nodes to which prepare
    * was sent. If the commit/rollback is to be sent in the same topologyId, then the 'recipients' param is returned back.
    * If the current topologyId is different than the topologyId of this transaction ({@link #getTopologyId()} then
    * this method returns the reunion between 'recipients' and {@link #getRemoteLocksAcquired()} from which it discards
    * the members that left.
    */
   public Collection<Address> getCommitNodes(Collection<Address> recipients, CacheTopology cacheTopology) {
      int currentTopologyId = cacheTopology.getTopologyId();
      List<Address> members = cacheTopology.getMembers();
      if (log.isTraceEnabled()) log.tracef("getCommitNodes recipients=%s, currentTopologyId=%s, members=%s, txTopologyId=%s",
                            recipients, currentTopologyId, members, getTopologyId());
      if (recipients == null) {
         return null;
      }
      // Include all the nodes we sent a LockControlCommand to and are not in the recipients list now
      // either because the topology changed, or because the lock failed.
      // Also include nodes that are no longer in the cluster, so if JGroups retransmits a lock/prepare command
      // after a merge, it also retransmits the commit/rollback.
      Set<Address> allRecipients = new HashSet<>(getRemoteLocksAcquired());
      allRecipients.addAll(recipients);
      if (log.isTraceEnabled()) log.tracef("The merged list of nodes to send commit/rollback is %s", allRecipients);
      return allRecipients;
   }

   /**
    * Sets the prepare sent for this transaction
    */
   public final void markPrepareSent() {
      prepareSent = true;
   }

   /**
    * @return  true if the prepare was sent to the other nodes
    */
   public final boolean isPrepareSent() {
      return prepareSent;
   }

   /**
    * @return {@code true} if there is an {@link IracMetadata} stored for {@code key}.
    */
   public synchronized boolean hasIracMetadata(Object key) {
      return iracMetadata != null && iracMetadata.containsKey(key);
   }

   /**
    * Stores the {@link IracMetadata} associated with {@code key}.
    *
    * @param key      The key.
    * @param metadata The {@link CompletionStage} that will be completed with {@link IracMetadata} to associate.
    */
   public synchronized void storeIracMetadata(Object key, CompletionStage<IracMetadata> metadata) {
      if (iracMetadata == null) {
         iracMetadata = new HashMap<>();
      }
      CompletionStage<IracMetadata> old = iracMetadata.put(key, metadata);
      assert old == null : "[IRAC] irac metadata replaced!";
   }

   /**
    * @return The {@link IracMetadata} associated with {@code key}.
    */
   public synchronized CompletionStage<IracMetadata> getIracMetadata(Object key) {
      return iracMetadata == null ? null : iracMetadata.get(key);
   }

}
