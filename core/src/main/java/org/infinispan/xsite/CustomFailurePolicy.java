package org.infinispan.xsite;

import java.util.Collection;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.transaction.Transaction;

import org.infinispan.Cache;

/**
 * Used for implementing custom policies in case of communication failures with a remote site. The handle methods are
 * allowed to throw instances of {@link BackupFailureException} to signal that they want the intra-site operation to
 * fail as well. If handle methods don't throw any exception then the operation will succeed in the local cluster. For
 * convenience, there is a support implementation of this class: {@link AbstractCustomFailurePolicy}
 * <p>
 * Lifecycle: the same instance is invoked during the lifecycle of a cache so it is allowed to hold state between
 * invocations.
 * <p>
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

   default void handleComputeFailure(String site, K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, boolean computeIfPresent) {}

   default void handleComputeIfAbsentFailure(String site, K key, Function<? super K, ? extends V> mappingFunction) {}

   default void handleReadWriteKeyFailure(String site, K key) {}

   default void handleReadWriteKeyValueFailure(String site, K key) {}

   default void handleWriteOnlyKeyFailure(String site, K key) {}

   default void handleWriteOnlyKeyValueFailure(String site, K key) {}

   default void handleReadWriteManyFailure(String site, Collection<? extends K> keys) {}

   default void handleReadWriteManyEntriesFailure(String site, Map<? extends K, ? extends V> keys) {}

   default void handleWriteOnlyManyFailure(String site, Collection<? extends K> key) {}

   default void handleWriteOnlyManyEntriesFailure(String site, Map<? extends K, ? extends V> key) {}

   void handleClearFailure(String site);

   void handlePutAllFailure(String site, Map<K, V> map);

   void handlePrepareFailure(String site, Transaction transaction);

   void handleRollbackFailure(String site, Transaction transaction);

   void handleCommitFailure(String site, Transaction transaction);
}
