package org.infinispan.context.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalTransaction;
import org.infinispan.util.BidirectionalMap;

import javax.transaction.Transaction;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Invocation context to be used for locally originated transactions.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class LocalTxInvocationContext extends AbstractTxInvocationContext {

   private volatile LocalTransaction localTransaction;

   public Transaction getRunningTransaction() {
      return localTransaction.getTransaction();
   }

   public boolean isOriginLocal() {
      return true;
   }

   public boolean isInTxScope() {
      return true;
   }

   public Object getLockOwner() {
      return localTransaction.getGlobalTransaction();
   }

   public GlobalTransaction getGlobalTransaction() {
      return localTransaction.getGlobalTransaction();
   }

   public List<WriteCommand> getModifications() {
      return localTransaction == null ? null : localTransaction.getModifications();
   }

   public void setLocalTransaction(LocalTransaction localTransaction) {
      this.localTransaction = localTransaction;
   }

   public CacheEntry lookupEntry(Object key) {
      return localTransaction != null ? localTransaction.lookupEntry(key) : null;
   }

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return localTransaction.getLookedUpEntries();
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      localTransaction.putLookedUpEntry(key, e);
   }

   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      for (Map.Entry<Object, CacheEntry> ce: lookedUpEntries.entrySet()) {
         localTransaction.putLookedUpEntry(ce.getKey(), ce.getValue());
      }
   }

   public void removeLookedUpEntry(Object key) {
      localTransaction.removeLookedUpEntry(key);
   }

   public void clearLookedUpEntries() {
      localTransaction.clearLookedUpEntries();
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return localTransaction != null && super.hasLockedKey(key);
   }

   public void remoteLocksAcquired(Collection<Address> nodes) {
      localTransaction.locksAcquired(nodes);
   }
   
   public Collection<Address> getRemoteLocksAcquired(){
	   return localTransaction.getRemoteLocksAcquired();
   }
}
