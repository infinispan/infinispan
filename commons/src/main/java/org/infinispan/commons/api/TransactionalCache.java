package org.infinispan.commons.api;

import javax.transaction.TransactionManager;

/**
 * This interface is implemented by caches that support (i.e. can interact with) transactions.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public interface TransactionalCache {

   /**
    * @return the {@link TransactionManager} in use by this cache or {@code null} if the cache isn't transactional.
    */
   default TransactionManager getTransactionManager() {
      return null;
   }

}
