package org.infinispan.context.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.infinispan.util.BidirectionalMap;

import javax.transaction.Transaction;
import java.util.List;
import java.util.Map;

/**
 * // TODO: Mircea: Document this!
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class InitiatorTxInvocationContext extends AbstractTxInvocationContext {

   private TransactionXaAdapter xaAdapter;

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
      return xaAdapter.getTransactionIdentifier();
   }

   public GlobalTransaction getClusterTransactionId() {
      return xaAdapter.getTransactionIdentifier();
   }

   public List<WriteCommand> getModifications() {
      return xaAdapter.getModifications();
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
      xaAdapter.putLookedUpEntries(lookedUpEntries);
   }

   public void removeLookedUpEntry(Object key) {
      xaAdapter.removeLookedUpEntry(key);
   }

   public void clearLookedUpEntries() {
      xaAdapter.clearLookedUpEntries();
   }
}
