package org.infinispan.xsite;

import java.util.Map;

import javax.transaction.Transaction;

import org.infinispan.Cache;

/**
 * Used for implementing custom policies in case of communication failures with a remote site. The handle methods are
 * allowed to throw instances of {@link BackupFailureException} to signal that they want the intra-site operation to
 * fail as well. If handle methods don't throw any exception then the operation will succeed in the local cluster. For
 * convenience, there is a support implementation of this class: {@link AbstractCustomFailurePolicy}
 * <p/>
 * Lifecycle: the same instance is invoked during the lifecycle of a cache so it is allowed to hold state between
 * invocations.
 * <p/>
 * Threadsafety: instances of this class might be invoked from different threads and they should be synchronized.
 *
 * @author Mircea Markus
 * @see BackupFailureException
 * @since 5.2
 */
public interface CustomFailurePolicy<K, V> {

   /**
    * Invoked during the initialization phase.
    */
   void init(Cache<K, V> cache);

   void handlePutFailure(String site, K key, V value, boolean putIfAbsent);

   void handleRemoveFailure(String site, K key, V oldValue);

   void handleReplaceFailure(String site, K key, V oldValue, V newValue);

   void handleClearFailure(String site);

   void handlePutAllFailure(String site, Map<K, V> map);

   void handlePrepareFailure(String site, Transaction transaction);

   void handleRollbackFailure(String site, Transaction transaction);

   void handleCommitFailure(String site, Transaction transaction);
}
