package org.infinispan.transaction.xa;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.context.impl.TxInvocationContext;

/**
 * Defines the state a infinispan transaction should have.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface CacheTransaction {

   /**
    * Returns the transaction identifier.
    */
   GlobalTransaction getGlobalTransaction();

   /**
    * Returns the modifications visible within the current transaction. Any modifications using Flag#CACHE_MODE_LOCAL are excluded.
    * The returned list is never null.
    */
   List<WriteCommand> getModifications();

   /**
    * Returns all the modifications visible within the current transaction, including those using Flag#CACHE_MODE_LOCAL.
    * The returned list is never null.
    */
   List<WriteCommand> getAllModifications();

   /**
    * Checks if a modification of the given class (or subclass) is present in this transaction. Any modifications using Flag#CACHE_MODE_LOCAL are ignored.
    *
    * @param modificationClass the modification type to look for
    * @return true if found, false otherwise
    * @deprecated since 14.0. To be removed without replacement.
    */
   @Deprecated
   default boolean hasModification(Class<?> modificationClass) {
      return false;
   }

   CacheEntry lookupEntry(Object key);

   Map<Object, CacheEntry> getLookedUpEntries();

   void putLookedUpEntry(Object key, CacheEntry e);

   void putLookedUpEntries(Map<Object, CacheEntry> entries);

   void removeLookedUpEntry(Object key);

   void clearLookedUpEntries();

   boolean ownsLock(Object key);

   void clearLockedKeys();

   Set<Object> getLockedKeys();

   int getTopologyId();

   void addBackupLockForKey(Object key);

   /**
    * @see org.infinispan.interceptors.locking.AbstractTxLockingInterceptor#checkPendingAndLockKey(TxInvocationContext, VisitableCommand, Object, long)
    */
   void notifyOnTransactionFinished();

   Map<Object, IncrementableEntryVersion> getUpdatedEntryVersions();

   void setUpdatedEntryVersions(Map<Object, IncrementableEntryVersion> updatedEntryVersions);

   boolean isMarkedForRollback();

   void markForRollback(boolean markForRollback);

   /**
    * Sets the version read for this key. The version is only set at the first time, i.e. multiple invocation of this
    * method will not change the state.
    * <p/>
    * Note: used in Repeatable Read + Write Skew + Clustering + Versioning.
    */
   void addVersionRead(Object key, EntryVersion version);

   /**
    * Note: used in Repeatable Read + Write Skew + Clustering + Versioning.
    *
    * @return a non-null map between key and version. The map represents the version read for that key. If no version
    *         exists, the key has not been read.
    */
   Map<Object, IncrementableEntryVersion> getVersionsRead();

   long getCreationTime();

   void addListener(TransactionCompletedListener listener);

   interface TransactionCompletedListener {
      void onCompletion();
   }

   /**
    * Prevent new modifications after prepare or commit started.
    */
   void freezeModifications();


   /**
    * It returns a {@link CompletableFuture} that completes when the lock for the {@code key} is released.
    *
    * If the {@code key} is not locked by this transaction, it returns {@code null}.
    *
    * @param key the key.
    * @return the {@link CompletableFuture} or {@code null} if the key is not locked by this transaction.
    */
   CompletableFuture<Void> getReleaseFutureForKey(Object key);

   /**
    * Same as {@link #getReleaseFutureForKey(Object)} but it returns a pair with the key and the future.
    */
   Map<Object, CompletableFuture<Void>> getReleaseFutureForKeys(Collection<Object> keys);

   /**
    * It cleans up the backup locks for this transaction.
    */
   void cleanupBackupLocks();

   /**
    * It cleans up the backup lock for the {@code keys}.
    *
    * @param keys The keys to clean up the backup lock.
    */
   void removeBackupLocks(Collection<?> keys);

   /**
    * It cleans up the backup for {@code key}.
    *
    * @param key The key to clean up the backup lock.
    */
   void removeBackupLock(Object key);

   /**
    * Invokes the {@link Consumer} with each lock.
    * @param consumer The backup lock {@link Consumer}
    */
   void forEachLock(Consumer<Object> consumer);

   /**
    * Invokes the {@link Consumer} with each backup lock.
    * @param consumer The backup lock {@link Consumer}
    */
   void forEachBackupLock(Consumer<Object> consumer);
}
