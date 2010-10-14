package org.infinispan.context.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionXaAdapter;
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

   private volatile TransactionXaAdapter xaAdapter;

   public Transaction getRunningTransaction() {
      return xaAdapter.getTransaction();
   }

   public boolean isOriginLocal() {
      return true;
   }

   public boolean isInTxScope() {
      return true;
   }

   public Object getLockOwner() {
      return xaAdapter.getGlobalTx();
   }

   public GlobalTransaction getGlobalTransaction() {
      return xaAdapter.getGlobalTx();
   }

   public List<WriteCommand> getModifications() {
      return xaAdapter == null ? null : xaAdapter.getModifications();
   }

   public void setXaCache(TransactionXaAdapter xaAdapter) {
      this.xaAdapter = xaAdapter;
   }

   public CacheEntry lookupEntry(Object key) {
      return xaAdapter != null ? xaAdapter.lookupEntry(key) : null;
   }

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return xaAdapter.getLookedUpEntries();
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      xaAdapter.putLookedUpEntry(key, e);
   }

   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      for (Map.Entry<Object, CacheEntry> ce: lookedUpEntries.entrySet()) {
         xaAdapter.putLookedUpEntry(ce.getKey(), ce.getValue());
      }
   }

   public void removeLookedUpEntry(Object key) {
      xaAdapter.removeLookedUpEntry(key);
   }

   public void clearLookedUpEntries() {
      xaAdapter.clearLookedUpEntries();
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return xaAdapter != null && super.hasLockedKey(key);
   }

   public void remoteLocksAcquired(Collection<Address> nodes) {
      xaAdapter.locksAcquired(nodes);
   }
}
