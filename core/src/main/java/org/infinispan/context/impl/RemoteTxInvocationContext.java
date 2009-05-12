package org.infinispan.context.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.BidirectionalMap;
import org.infinispan.util.BidirectionalLinkedHashMap;

import javax.transaction.Transaction;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * // TODO: Mircea: Document this!
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class RemoteTxInvocationContext extends AbstractTxInvocationContext {

   private List<WriteCommand> modifications;

   private BidirectionalMap<Object, CacheEntry> lookedUpEntries;

   protected GlobalTransaction tx;

   public RemoteTxInvocationContext() {
   }

   public Transaction getRunningTransaction() {
      throw new IllegalStateException("this method can only be called for locally originated transactions!");
   }

   public Object getLockOwner() {
      return tx;
   }

   public GlobalTransaction getClusterTransactionId() {
      return tx;
   }

   public boolean isInTxScope() {
      return true;
   }

   public boolean isOriginLocal() {
      return false;
   }

   public List<WriteCommand> getModifications() {
      return modifications;
   }

   public void initialize(WriteCommand[] modifications, GlobalTransaction tx) {
      this.modifications = Arrays.asList(modifications);
      lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(modifications.length);
      this.tx = tx;
   }

   public CacheEntry lookupEntry(Object key) {
      return lookedUpEntries.get(key);
   }

   public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
      return lookedUpEntries;
   }

   public void putLookedUpEntry(Object key, CacheEntry e) {
      lookedUpEntries.put(key, e);
   }

   public void removeLookedUpEntry(Object key) {
      lookedUpEntries.remove(key);
   }

   public void clearLookedUpEntries() {
      lookedUpEntries.clear();
   }

   public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
      lookedUpEntries.putAll(lookedUpEntries);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof RemoteTxInvocationContext)) return false;

      RemoteTxInvocationContext context = (RemoteTxInvocationContext) o;
      return tx.equals(context.tx);
   }

   @Override
   public int hashCode() {
      return tx.hashCode();
   }

   @Override
   public String toString() {
      return "RemoteTxInvocationContext{" +
            "modifications=" + modifications +
            ", lookedUpEntries=" + lookedUpEntries +
            ", tx=" + tx +
            "} " + super.toString();
   }

   @Override
   public Object clone() {
      RemoteTxInvocationContext dolly = (RemoteTxInvocationContext) super.clone();
      if (modifications != null) {
         dolly.modifications = new ArrayList<WriteCommand>(modifications);
      }
      if (lookedUpEntries != null) {
         dolly.lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(lookedUpEntries);
      }
      if (tx != null) {
         dolly.tx = (GlobalTransaction) tx.clone();
      }
      return dolly;
   }
}
