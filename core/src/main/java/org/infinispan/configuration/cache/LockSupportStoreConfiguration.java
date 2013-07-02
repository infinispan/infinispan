package org.infinispan.configuration.cache;

/**
 * LockSupportStoreConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface LockSupportStoreConfiguration extends CacheStoreConfiguration {

   /**
    * The timeout in milliseconds before giving up on acquiring a lock
    *
    * @return
    */
   long lockAcquistionTimeout();

   /**
    * This value determines the number of threads that can concurrently access the lock container
    *
    * @return
    */
   int lockConcurrencyLevel();

}