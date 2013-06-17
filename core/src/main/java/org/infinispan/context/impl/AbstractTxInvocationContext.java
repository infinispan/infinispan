package org.infinispan.context.impl;

import javax.transaction.Transaction;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.transaction.AbstractCacheTransaction;

import java.util.Collection;
import java.util.Set;

/**
 * Support class for {@link org.infinispan.context.impl.TxInvocationContext}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class AbstractTxInvocationContext extends AbstractInvocationContext implements TxInvocationContext {

   private Transaction transaction;

   private boolean implicitTransaction;

   @Override
   public boolean hasModifications() {
      return getModifications() != null && !getModifications().isEmpty();
   }

   @Override
   public Set<Object> getAffectedKeys() {
      return getCacheTransaction().getAffectedKeys();
   }

   @Override
   public void addAllAffectedKeys(Collection<Object> keys) {
      if (keys != null && !keys.isEmpty()) {
         getCacheTransaction().addAllAffectedKeys(keys);
      }
   }

   @Override
   public void addAffectedKey(Object key) {
      getCacheTransaction().addAffectedKey(key);
   }

   @Override
   public void setImplicitTransaction(boolean implicit) {
      this.implicitTransaction = implicit;
   }

   @Override
   public boolean isImplicitTransaction() {
      return this.implicitTransaction;
   }

   @Override
   public boolean isInTxScope() {
      return true;
   }

   public TxInvocationContext setTransaction(Transaction transaction) {
      this.transaction = transaction;
      return this;
   }

   @Override
   public Transaction getTransaction() {
      return transaction;
   }

   @Override
   public final void clearLockedKeys() {
      getCacheTransaction().clearLockedKeys();
   }

   @Override
   protected void onEntryValueReplaced(Object key, InternalCacheEntry cacheEntry) {
      //the value to be returned was read from remote node. We need to update the version seen.
      getCacheTransaction().replaceVersionRead(key, cacheEntry.getMetadata().version());
   }

   @Override
   public abstract AbstractCacheTransaction getCacheTransaction();

}
