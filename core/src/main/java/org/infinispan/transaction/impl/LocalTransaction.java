package org.infinispan.transaction.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.transaction.Transaction;

import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Object that holds transaction's state on the node where it originated; as opposed to {@link RemoteTransaction}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.0
 */
public abstract class LocalTransaction extends AbstractCacheTransaction {

   private static final Log log = LogFactory.getLog(LocalTransaction.class);
   private static final boolean trace = log.isTraceEnabled();

   private Set<Address> remoteLockedNodes;
   private Set<Object> readKeys = null;

   private final Transaction transaction;

   private final boolean implicitTransaction;

   private volatile boolean isFromRemoteSite;

   private boolean prepareSent;
   private boolean commitOrRollbackSent;

   public LocalTransaction(Transaction transaction, GlobalTransaction tx,
         boolean implicitTransaction, int topologyId, Equivalence<Object> keyEquivalence, long txCreationTime) {
      super(tx, topologyId, keyEquivalence, txCreationTime);
      this.transaction = transaction;
      this.implicitTransaction = implicitTransaction;
   }

   public final void addModification(WriteCommand mod) {
      if (trace) log.tracef("Adding modification %s. Mod list is %s", mod, modifications);
      if (modifications == null) {
         // we need to synchronize this collection to be able to get a valid snapshot from another thread during state transfer
         //modifications = Collections.synchronizedList(new LinkedList<WriteCommand>());
         modifications = CollectionFactory.makeSynchronizedList();
      }
      if (mod.hasFlag(Flag.CACHE_MODE_LOCAL)) {
         hasLocalOnlyModifications = true;
      }
      modifications.add(mod);
   }

   public void locksAcquired(Collection<Address> nodes) {
      if (trace) log.tracef("Adding remote locks on %s. Remote locks are %s", nodes, remoteLockedNodes);
      if (remoteLockedNodes == null)
         remoteLockedNodes = CollectionFactory.makeSet(nodes);
      else
         remoteLockedNodes.addAll(nodes);
   }

   public Collection<Address> getRemoteLocksAcquired(){
	   if (remoteLockedNodes == null) return InfinispanCollections.emptySet();
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
      return lookedUpEntries == null ? InfinispanCollections.<Object, CacheEntry>emptyMap() : lookedUpEntries;
   }

   public boolean isImplicitTransaction() {
      return implicitTransaction;
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      if (isMarkedForRollback()) {
         throw new CacheException("This transaction is marked for rollback and cannot acquire locks!");
      }
      if (lookedUpEntries == null)
         lookedUpEntries = CollectionFactory.makeMap(4, keyEquivalence, AnyEquivalence.<CacheEntry>getInstance());

      lookedUpEntries.put(key, e);
   }

   @Override
   public void putLookedUpEntries(Map<Object, CacheEntry> entries) {
      if (isMarkedForRollback()) {
         throw new CacheException("This transaction is marked for rollback and cannot acquire locks!");
      }
      if (lookedUpEntries == null) {
         lookedUpEntries = CollectionFactory.makeMap(entries, keyEquivalence, AnyEquivalence.<CacheEntry>getInstance());
      } else {
         lookedUpEntries.putAll(entries);
      }
   }

   public boolean isReadOnly() {
      return modifications == null || modifications.isEmpty();
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
            ", lockedKeys=" + lockedKeys +
            ", backupKeyLocks=" + backupKeyLocks +
            ", topologyId=" + topologyId +
            ", stateTransferFlag=" + getStateTransferFlag() +
            "} " + super.toString();
   }

   @Override
   public void addReadKey(Object key) {
      if (readKeys == null) readKeys = new HashSet<Object>(2);
      readKeys.add(key);
   }

   @Override
   public boolean keyRead(Object key) {
      return readKeys != null && readKeys.contains(key);
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
   public Collection<Address> getCommitNodes(Collection<Address> recipients, int currentTopologyId, Collection<Address> members) {
      if (trace) log.tracef("getCommitNodes recipients=%s, currentTopologyId=%s, members=%s, txTopologyId=%s",
                            recipients, currentTopologyId, members, getTopologyId());
      if (hasModification(ClearCommand.class)) {
         return members;
      }
      if (recipients == null) {
         return null;
      }
      if (getTopologyId() == currentTopologyId) {
         return recipients;
      }
      Set<Address> allRecipients = new HashSet<Address>(getRemoteLocksAcquired());
      allRecipients.addAll(recipients);
      allRecipients.retainAll(members);
      if (trace) log.tracef("The merged list of nodes to send commit/rollback is %s", allRecipients);
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
    * Sets the commit or rollback sent for this transaction
    */
   public final void markCommitOrRollbackSent() {
      commitOrRollbackSent = true;
   }

   /**
    * @return  true if the commit or rollback was sent to the other nodes
    */
   public final boolean isCommitOrRollbackSent() {
      return commitOrRollbackSent;
   }

}
