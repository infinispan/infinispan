package org.infinispan.xsite;

import java.util.Map;

import javax.transaction.Transaction;

import org.infinispan.Cache;

/**
 * Support class for {@link CustomFailurePolicy}.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public abstract class AbstractCustomFailurePolicy<K,V> implements CustomFailurePolicy<K,V> {

   protected volatile Cache<K,V> cache;

   @Override
   public void init(Cache cache) {
      this.cache = cache;
   }

   @Override
   public void handlePutFailure(String site, K key, V value, boolean putIfAbsent) {
   }

   @Override
   public void handleRemoveFailure(String site, K key, V oldValue) {
   }

   @Override
   public void handleReplaceFailure(String site, K key, V oldValue, V newValue) {
   }

   @Override
   public void handleClearFailure(String site) {
   }

   @Override
   public void handlePutAllFailure(String site, Map<K, V> map) {
   }

   @Override
   public void handlePrepareFailure(String site, Transaction transaction) {
   }

   @Override
   public void handleRollbackFailure(String site, Transaction transaction) {
   }

   @Override
   public void handleCommitFailure(String site, Transaction transaction) {
   }
}
