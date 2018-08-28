package org.infinispan.server.hotrod.tx.table;

import org.infinispan.server.hotrod.tx.table.functions.SetDecisionFunction;
import org.infinispan.util.ByteString;

/**
 * Used by {@link GlobalTxTable}, it collects all the involved cache name when setting a decision for a transaction.
 * <p>
 * Initially, {@link #expectedSize(int)} is invoked with the number of caches found. For all cache, it {@link TxState}
 * is updated with the decision (via {@link SetDecisionFunction}) and {@link #addCache(ByteString, Status)} is invoked
 * with the cache name and the function return value.
 * <p>
 * If no transaction is found, only {@link #noTransactionFound()} is invoked.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public interface CacheNameCollector {

   /**
    * Sets the expected number of caches involved in the transaction.
    */
   void expectedSize(int size);

   /**
    * Adds the cache name and the {@link SetDecisionFunction} return value.
    */
   void addCache(ByteString cacheName, Status status);

   /**
    * Notifies that no transaction is found.
    */
   void noTransactionFound();

}
