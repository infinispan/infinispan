package org.infinispan.loaders;


/**
 * Adds configuration support for {@link LockSupportCacheStore}.
 *
 * @author Mircea.Markus@jboss.com
 */
public class LockSupportCacheStoreConfig extends AbstractCacheStoreConfig {

   private static final long serialVersionUID = 842757200078048889L;
   
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
   public void setLockConcurrencyLevel(int lockConcurrencyLevel) {
      testImmutability("lockConcurrencyLevel");
      this.lockConcurrencyLevel = lockConcurrencyLevel;
   }

   public long getLockAcquistionTimeout() {
      return lockAcquistionTimeout;
   }

   public void setLockAcquistionTimeout(long lockAcquistionTimeout) {
      testImmutability("lockAcquistionTimeout");
      this.lockAcquistionTimeout = lockAcquistionTimeout;
   }

   @Override
   public String toString() {
      return "LockSupportCacheStoreConfig{" +
            "lockConcurrencyLevel=" + lockConcurrencyLevel +
            ", lockAcquistionTimeout=" + lockAcquistionTimeout +
            "} " + super.toString();
   }
}
