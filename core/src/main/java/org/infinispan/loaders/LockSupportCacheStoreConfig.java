package org.infinispan.loaders;

import org.infinispan.config.ConfigurationProperty;

/**
 * Adds configuration support for {@link LockSupportCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class LockSupportCacheStoreConfig extends AbstractCacheStoreConfig {
   public static final int DEFAULT_CONCURRENCY_LEVEL = 2048;
   public static final int DEFAULT_LOCK_ACQUISITION_TIMEOUT = 60000;

   private int lockConcurrencyLevel = DEFAULT_CONCURRENCY_LEVEL;
   private long lockAcquistionTimeout = DEFAULT_LOCK_ACQUISITION_TIMEOUT;

   /**
    * Returns number of threads expected to use this class concurrently.
    */
   public int getLockConcurrencyLevel() {
      return lockConcurrencyLevel;
   }

   /**
    * Sets number of threads expected to use this class concurrently.
    */
   @ConfigurationProperty(name = "lockConcurrencyLevel", 
            parentElement = "properties", 
            description = "Concurrency level as integer. Default is " + DEFAULT_CONCURRENCY_LEVEL)
   public void setLockConcurrencyLevel(int lockConcurrencyLevel) {
      testImmutability("lockConcurrencyLevel");
      this.lockConcurrencyLevel = lockConcurrencyLevel;
   }

   public long getLockAcquistionTimeout() {
      return lockAcquistionTimeout;
   }

   @ConfigurationProperty(name = "lockAcquistionTimeout", 
            parentElement = "properties",
            description= "Default lock acquisition timeout as long. Default is " + DEFAULT_LOCK_ACQUISITION_TIMEOUT)
   public void setLockAcquistionTimeout(long lockAcquistionTimeout) {
      testImmutability("lockAcquistionTimeout");
      this.lockAcquistionTimeout = lockAcquistionTimeout;
   }
}
