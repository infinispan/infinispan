package org.infinispan.transaction;

/**
 * Enumeration containing the available transaction modes for a cache.
 *
 * Starting with Infinispan version 5.1 a cache doesn't support mixed access:
 * i.e. won't support transactional and non-transactional operations.
 * A cache is transactional if one the following:
 *
 * <pre>
 * - a transactionManagerLookup is configured for the cache
 * - batching is enabled
 * - it is explicitly marked as transactional: config.fluent().transaction().transactionMode(TransactionMode.TRANSACTIONAL).
 *   In this last case a transactionManagerLookup needs to be explicitly set
 * </pre>
 *
 * By default a cache is not transactional.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public enum TransactionMode {
   NON_TRANSACTIONAL,
   TRANSACTIONAL;

   public boolean isTransactional() {
      return this == TRANSACTIONAL;
   }
}
