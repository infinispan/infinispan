package org.infinispan.context.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.equivalence.AnyEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.AbstractCacheTransaction;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Invocation context to be used for locally originated transactions.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public class LocalTxInvocationContext extends AbstractTxInvocationContext {

   public final Map<Object, CacheEntry> emptyEntryMap;

   private LocalTransaction localTransaction;

   public LocalTxInvocationContext(Equivalence<Object> keyEq) {
      emptyEntryMap = CollectionFactory.makeMap(0, keyEq, AnyEquivalence.<CacheEntry>getInstance());
   }

   @Override
   public boolean isTransactionValid() {
      Transaction t = getTransaction();
      int status = -1;
      if (t != null) {
         try {
            status = t.getStatus();
         } catch (SystemException e) {
            // no op
         }
      }
      return status == Status.STATUS_ACTIVE || status == Status.STATUS_PREPARING;
   }

   @Override
   public boolean isOriginLocal() {
      return true;
   }

   @Override
   public Object getLockOwner() {
      return localTransaction.getGlobalTransaction();
   }

   @Override
   public GlobalTransaction getGlobalTransaction() {
      return localTransaction.getGlobalTransaction();
   }

   @Override
   public List<WriteCommand> getModifications() {
      return localTransaction == null ? null : localTransaction.getModifications();
   }

   public void setLocalTransaction(LocalTransaction localTransaction) {
      this.localTransaction = localTransaction;
   }

   @Override
   public CacheEntry lookupEntry(Object key) {
      return localTransaction != null ? localTransaction.lookupEntry(key) : null;
   }

   @Override
   public Map<Object, CacheEntry> getLookedUpEntries() {
      return localTransaction != null ? localTransaction.getLookedUpEntries() : emptyEntryMap;
   }

   @Override
   public void putLookedUpEntry(Object key, CacheEntry e) {
      localTransaction.putLookedUpEntry(key, e);
   }

   @Override
   public void removeLookedUpEntry(Object key) {
      localTransaction.removeLookedUpEntry(key);
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return localTransaction != null && localTransaction.ownsLock(key);
   }

   public void remoteLocksAcquired(Collection<Address> nodes) {
      localTransaction.locksAcquired(nodes);
   }

   public Collection<Address> getRemoteLocksAcquired() {
      return localTransaction.getRemoteLocksAcquired();
   }

   @Override
   public AbstractCacheTransaction getCacheTransaction() {
      return localTransaction;
   }

   @Override
   public void skipTransactionCompleteCheck(boolean skip) {
      //no-op
   }

   @Override
   public boolean skipTransactionCompleteCheck() {
      //no-op. the check is only performed in remote transactions
      return true;
   }

   @Override
   public Set<Object> getLockedKeys() {
      return localTransaction == null ?
            InfinispanCollections.emptySet() : localTransaction.getLockedKeys();
   }

   @Override
   public void addLockedKey(Object key) {
      localTransaction.registerLockedKey(key);
   }

   @Override
   public Transaction getTransaction() {
      Transaction tx = super.getTransaction();
      return tx == null ? localTransaction.getTransaction() : tx;
   }
}
