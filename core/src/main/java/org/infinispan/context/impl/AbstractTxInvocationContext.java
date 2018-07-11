package org.infinispan.context.impl;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.AbstractCacheTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;

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
   private Object lockOwnerOverride;

   protected AbstractTxInvocationContext(T cacheTransaction, Address origin) {
      super(origin);
      if (cacheTransaction == null) {
         throw new NullPointerException("CacheTransaction cannot be null");
      }
      this.cacheTransaction = cacheTransaction;
   }

   @Override
   public Object getLockOwner() {
      if (lockOwnerOverride != null) {
         return lockOwnerOverride;
      }
      //not final because the test suite overwrite it...
      return cacheTransaction.getGlobalTransaction();
   }

   @Override
   public void setLockOwner(Object lockOwner) {
      lockOwnerOverride = lockOwner;
   }

   @Override
   public final Set<Object> getLockedKeys() {
      return cacheTransaction.getLockedKeys();
   }

   @Override
   public boolean hasLockedKey(Object key) {
      return cacheTransaction.ownsLock(key);
   }

   @Override
   public void forEachLock(Consumer<Object> consumer) {
      cacheTransaction.forEachLock(consumer);
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
      List<WriteCommand> replicableModifications = getModifications();
      return replicableModifications != null && !replicableModifications.isEmpty();
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
      throw new UnsupportedOperationException();
   }

   @Override
   public void forEachValue(BiConsumer<Object, CacheEntry> action) {
      cacheTransaction.forEachValue(action);
   }

   @Override
   public void forEachEntry(BiConsumer<Object, CacheEntry> action) {
      cacheTransaction.forEachEntry(action);
   }

   @Override
   public int lookedUpEntriesCount() {
      return cacheTransaction.lookedUpEntriesCount();
   }

   @Override
   public final Set<Object> getAffectedKeys() {
      return cacheTransaction.getAffectedKeys();
   }

   @Override
   public final void addAllAffectedKeys(Collection<?> keys) {
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
   public final T getCacheTransaction() {
      return cacheTransaction;
   }
}
