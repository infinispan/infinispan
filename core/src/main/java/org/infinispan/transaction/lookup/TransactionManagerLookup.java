package org.infinispan.transaction.lookup;

import javax.transaction.TransactionManager;

/**
 * Factory interface, allows {@link org.infinispan.Cache} to use different transactional systems. Names of implementors of
 * this class can be configured using {@link Configuration#setTransactionManagerLookupClass}.
 * Thread safety: it is possible for the same instance of this class to be used by multiple caches at the same time e.g.
 * when the same instance is passed to multiple configurations:
 * {@link org.infinispan.configuration.cache.TransactionConfigurationBuilder#transactionManagerLookup(TransactionManagerLookup)}.
 * As infinispan supports parallel test startup, it might be possible for multiple threads to invoke the
 * getTransactionManager() method concurrently, so it is highly recommended for instances of this class to be thread safe.
 *
 * @author Bela Ban, Aug 26 2003
 * @since 4.0
 */
public interface TransactionManagerLookup {

   /**
    * Returns a new TransactionManager.
    *
    * @throws Exception if lookup failed
    */
   TransactionManager getTransactionManager() throws Exception;

}
