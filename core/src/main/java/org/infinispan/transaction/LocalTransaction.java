package org.infinispan.transaction;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.BidirectionalLinkedHashMap;
import org.infinispan.util.BidirectionalMap;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Object that holds transaction's state on the node where it originated; as opposed to {@link RemoteTransaction}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public abstract class LocalTransaction extends AbstractCacheTransaction {

   private static Log log = LogFactory.getLog(LocalTransaction.class);
   private static final boolean trace = log.isTraceEnabled();

   private Set<Address> remoteLockedNodes;

   /** mark as volatile as this might be set from the tx thread code on view change*/
   private volatile boolean isMarkedForRollback;

   private final Transaction transaction;

   public LocalTransaction(Transaction transaction, GlobalTransaction tx) {
      super.tx = tx;
      this.transaction = transaction;
   }

   public void addModification(WriteCommand mod) {
      if (trace) log.trace("Adding modification %s. Mod list is %s", mod, modifications);
      if (modifications == null) {
         modifications = new LinkedList<WriteCommand>();
      }
      modifications.add(mod);
   }

   public boolean hasRemoteLocksAcquired(List<Address> leavers) {
      if (log.isTraceEnabled()) {
         log.trace("My remote locks: " + remoteLockedNodes + ", leavers are:" + leavers);
      }
      return (remoteLockedNodes != null) && !Collections.disjoint(remoteLockedNodes, leavers);
   }

   public void locksAcquired(Collection<Address> nodes) {
      if (remoteLockedNodes == null) remoteLockedNodes = new HashSet<Address>();
      remoteLockedNodes.addAll(nodes);
   }

   public Collection<Address> getRemoteLocksAcquired(){
	   if (remoteLockedNodes == null) return Collections.emptySet();
	   return remoteLockedNodes;
   }

   public void markForRollback() {
      isMarkedForRollback = true;
   }

   public boolean isMarkedForRollback() {
      return isMarkedForRollback;
   }

   public Transaction getTransaction() {
      return transaction;
   }

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return (BidirectionalMap<Object, CacheEntry>)
            (lookedUpEntries == null ? InfinispanCollections.emptyBidirectionalMap() : lookedUpEntries);
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      if (lookedUpEntries == null) lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(4);
      lookedUpEntries.put(key, e);
   }

   public boolean isReadOnly() {
      return (modifications == null || modifications.isEmpty()) && (lookedUpEntries == null || lookedUpEntries.isEmpty());
   }

   public abstract boolean isEnlisted();

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LocalTransaction that = (LocalTransaction) o;

      if (isMarkedForRollback != that.isMarkedForRollback) return false;
      if (remoteLockedNodes != null ? !remoteLockedNodes.equals(that.remoteLockedNodes) : that.remoteLockedNodes != null)
         return false;
      if (transaction != null ? !transaction.equals(that.transaction) : that.transaction != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = remoteLockedNodes != null ? remoteLockedNodes.hashCode() : 0;
      result = 31 * result + (isMarkedForRollback ? 1 : 0);
      result = 31 * result + (transaction != null ? transaction.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "LocalTransaction{" +
            "remoteLockedNodes=" + remoteLockedNodes +
            ", isMarkedForRollback=" + isMarkedForRollback +
            ", transaction=" + transaction +
            "} " + super.toString();
   }

}
