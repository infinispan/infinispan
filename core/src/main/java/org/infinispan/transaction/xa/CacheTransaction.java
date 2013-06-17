package org.infinispan.transaction.xa;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

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
    */
   boolean hasModification(Class<?> modificationClass);

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

   Set<Object> getBackupLockedKeys();

   void addBackupLockForKey(Object key);

   /**
    * @see org.infinispan.interceptors.locking.AbstractTxLockingInterceptor#lockKeyAndCheckOwnership(org.infinispan.context.InvocationContext, Object)
    */
   void notifyOnTransactionFinished();

   /**
    * Checks if this transaction holds a lock on the given key and then waits until the transaction completes or until
    * the timeout expires and returns <code>true</code> if the transaction is complete or <code>false</code> otherwise.
    * If the key is not locked or if the transaction is already completed it returns <code>true</code> immediately.
    * <p/>
    * This method is subject to spurious returns in a way similar to {@link java.lang.Object#wait()}. It can sometimes return
    * before the specified time has elapsed and without guaranteeing that this transaction is complete. The caller is
    * responsible to call the method again if transaction completion was not reached and the time budget was not spent.
    *
    * @see org.infinispan.interceptors.locking.AbstractTxLockingInterceptor#lockKeyAndCheckOwnership(org.infinispan.context.InvocationContext, Object)
    */
   boolean waitForLockRelease(Object key, long lockAcquisitionTimeout) throws InterruptedException;

   EntryVersionsMap getUpdatedEntryVersions();

   void setUpdatedEntryVersions(EntryVersionsMap updatedEntryVersions);

   void putLookedUpRemoteVersion(Object key, EntryVersion version);

   EntryVersion getLookedUpRemoteVersion(Object key);

   boolean keyRead(Object key);

   void addReadKey(Object key);

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
    * Sets the version read fr this key, replacing the old version if it exists, i.e each invocation updates the version
    * of the key. This method is used when a remote get is performed for the key.
    * <p/>
    * Note: used in Repeatable Read + Write Skew + Clustering + Versioning.
    */
   void replaceVersionRead(Object key, EntryVersion version);

   /**
    * Note: used in Repeatable Read + Write Skew + Clustering + Versioning.
    *
    * @return a non-null map between key and version. The map represents the version read for that key. If no version
    *         exists, the key has not been read.
    */
   EntryVersionsMap getVersionsRead();
}
