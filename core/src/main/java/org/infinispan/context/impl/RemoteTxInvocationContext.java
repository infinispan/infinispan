package org.infinispan.context.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.RemoteTransaction;
import org.infinispan.util.BidirectionalMap;

import javax.transaction.Transaction;
import java.util.List;
import java.util.Map;

/**
 * Context to be used for transaction that originated remotelly.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class RemoteTxInvocationContext extends AbstractTxInvocationContext {


   private RemoteTransaction remoteTransaction;

   public RemoteTxInvocationContext() {
   }

   public Transaction getRunningTransaction() {
      throw new IllegalStateException("this method can only be called for locally originated transactions!");
   }

   public Object getLockOwner() {
      return remoteTransaction.getGlobalTransaction();
   }

   public GlobalTransaction getGlobalTransaction() {
      return remoteTransaction.getGlobalTransaction();
   }

   public boolean isInTxScope() {
      return true;
   }

   public boolean isOriginLocal() {
      return false;
   }

   public List<WriteCommand> getModifications() {
      return remoteTransaction.getModifications();
   }

   public void setRemoteTransaction(RemoteTransaction remoteTransaction) {
      this.remoteTransaction = remoteTransaction;
   }

   public CacheEntry lookupEntry(Object key) {
      return remoteTransaction.lookupEntry(key);
   }

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return remoteTransaction.getLookedUpEntries();
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      keyAddedInCurrentInvocation(key);
      remoteTransaction.putLookedUpEntry(key, e);
   }

   public void removeLookedUpEntry(Object key) {
      remoteTransaction.removeLookedUpEntry(key);
   }

   public void clearLookedUpEntries() {
      remoteTransaction.clearLookedUpEntries();
   }

   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      for (Map.Entry<Object, CacheEntry> ce: lookedUpEntries.entrySet()) {
         keyAddedInCurrentInvocation(ce.getKey());
         remoteTransaction.putLookedUpEntry(ce.getKey(), ce.getValue());
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RemoteTxInvocationContext)) return false;
      RemoteTxInvocationContext that = (RemoteTxInvocationContext) o;
      return remoteTransaction.equals(that.remoteTransaction);
   }

   @Override
   public int hashCode() {
      return remoteTransaction.hashCode();
   }

   @Override
   public RemoteTxInvocationContext clone() {
      RemoteTxInvocationContext dolly = (RemoteTxInvocationContext) super.clone();
      dolly.remoteTransaction = (RemoteTransaction) remoteTransaction.clone();
      return dolly;
   }
}
