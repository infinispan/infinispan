package org.infinispan.context.impl;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Support class for {@link org.infinispan.context.impl.TxInvocationContext}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Pedro Ruivo
 * @since 4.0
 */
public abstract class AbstractTxInvocationContext<T extends AbstractCacheTransaction> extends AbstractInvocationContext
      implements TxInvocationContext<T> {

   private final T cacheTransaction;

   protected AbstractTxInvocationContext(T cacheTransaction) {
      if (cacheTransaction == null) {
         throw new NullPointerException("CacheTransaction cannot be null");
      }
      this.cacheTransaction = cacheTransaction;
   }

   @Override
   public Object getLockOwner() {
      //not final because the test suite overwrite it...
      return cacheTransaction.getGlobalTransaction();
   }

   @Override
   public final Set<Object> getLockedKeys() {
      return cacheTransaction.getLockedKeys();
   }

   @Override
   public final void addLockedKey(Object key) {
      cacheTransaction.registerLockedKey(key);
   }

   @Override
   public final GlobalTransaction getGlobalTransaction() {
      return cacheTransaction.getGlobalTransaction();
   }

   @Override
   public final boolean hasModifications() {
      return getModifications() != null && !getModifications().isEmpty();
   }

   @Override
   public final List<WriteCommand> getModifications() {
      return cacheTransaction.getModifications();
   }

   @Override
   public final CacheEntry lookupEntry(Object key) {
      return cacheTransaction.lookupEntry(key);
   }

   @Override
   public final Map<Object, CacheEntry> getLookedUpEntries() {
      return cacheTransaction.getLookedUpEntries();
   }

   @Override
   public final Set<Object> getAffectedKeys() {
      return cacheTransaction.getAffectedKeys();
   }

   @Override
   public final void addAllAffectedKeys(Collection<Object> keys) {
      if (keys != null && !keys.isEmpty()) {
         cacheTransaction.addAllAffectedKeys(keys);
      }
   }

   @Override
   public final void addAffectedKey(Object key) {
      cacheTransaction.addAffectedKey(key);
   }

   @Override
   public final void putLookedUpEntry(Object key, CacheEntry e) {
      cacheTransaction.putLookedUpEntry(key, e);
   }

   @Override
   public final void removeLookedUpEntry(Object key) {
      cacheTransaction.removeLookedUpEntry(key);
   }

   @Override
   public final boolean isInTxScope() {
      return true;
   }

   @Override
   public final void clearLockedKeys() {
      cacheTransaction.clearLockedKeys();
   }

   @Override
   protected final void onEntryValueReplaced(Object key, InternalCacheEntry cacheEntry) {
      //the value to be returned was read from remote node. We need to update the version seen.
      cacheTransaction.replaceVersionRead(key, cacheEntry.getMetadata().version());
   }

   @Override
   public final T getCacheTransaction() {
      return cacheTransaction;
   }
}
