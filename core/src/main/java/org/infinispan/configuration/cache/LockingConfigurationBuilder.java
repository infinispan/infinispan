package org.infinispan.configuration.cache;

import org.infinispan.config.ConfigurationException;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;

import java.util.concurrent.TimeUnit;

/**
 * Defines the local, in-VM locking and concurrency characteristics of the cache.
 * 
 * @author pmuir
 * 
 */
public class LockingConfigurationBuilder extends AbstractConfigurationChildBuilder<LockingConfiguration> {

   private int concurrencyLevel = 32;
   IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
   private long lockAcquisitionTimeout = TimeUnit.SECONDS.toMillis(10);
   private boolean useLockStriping = false;
   boolean writeSkewCheck = false;

   protected LockingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
      // TODO Auto-generated constructor stub
   }

   /**
    * Concurrency level for lock containers. Adjust this value according to the number of concurrent
    * threads interacting with Infinispan. Similar to the concurrencyLevel tuning parameter seen in
    * the JDK's ConcurrentHashMap.
    */
   public LockingConfigurationBuilder concurrencyLevel(int i) {
      this.concurrencyLevel = i;
      return this;
   }

   /**
    * Cache isolation level. Infinispan only supports READ_COMMITTED or REPEATABLE_READ isolation
    * levels. See <a href=
    * 'http://en.wikipedia.org/wiki/Isolation_level'>http://en.wikipedia.org/wiki/Isolation_level</a
    * > for a discussion on isolation levels.
    */
   public LockingConfigurationBuilder isolationLevel(IsolationLevel isolationLevel) {
      this.isolationLevel = isolationLevel;
      return this;
   }

   /**
    * Maximum time to attempt a particular lock acquisition
    */
   public LockingConfigurationBuilder lockAcquisitionTimeout(long l) {
      this.lockAcquisitionTimeout = l;
      return this;
   }

   /**
    * If true, a pool of shared locks is maintained for all entries that need to be locked.
    * Otherwise, a lock is created per entry in the cache. Lock striping helps control memory
    * footprint but may reduce concurrency in the system.
    */
   public LockingConfigurationBuilder useLockStriping(boolean b) {
      this.useLockStriping = b;
      return this;
   }

   /**
    * This setting is only applicable in the case of REPEATABLE_READ. When write skew check is set
    * to false, if the writer at commit time discovers that the working entry and the underlying
    * entry have different versions, the working entry will overwrite the underlying entry. If true,
    * such version conflict - known as a write-skew - will throw an Exception.
    */
   public LockingConfigurationBuilder writeSkewCheck(boolean b) {
      this.writeSkewCheck = b;
      return this;
   }

   @Override
   void validate() {
      if (writeSkewCheck) {
         if (isolationLevel != IsolationLevel.REPEATABLE_READ)
            throw new ConfigurationException("Write-skew checking only allowed with REPEATABLE_READ isolation level for cache");
         if (transaction().lockingMode != LockingMode.OPTIMISTIC)
            throw new ConfigurationException("Write-skew checking only allowed with OPTIMISTIC transactions");
         if (!versioning().enabled || versioning().scheme != VersioningScheme.SIMPLE)
            throw new ConfigurationException(
                  "Write-skew checking requires versioning to be enabled and versioning scheme 'SIMPLE' to be configured");
         if (clustering().cacheMode() != CacheMode.DIST_SYNC && clustering().cacheMode() != CacheMode.REPL_SYNC
               && clustering().cacheMode() != CacheMode.LOCAL)
            throw new ConfigurationException("Write-skew checking is only supported in REPL_SYNC, DIST_SYNC and LOCAL modes.  "
                  + clustering().cacheMode() + " cannot be used with write-skew checking");
      }
   }

   @Override
   LockingConfiguration create() {
      return new LockingConfiguration(concurrencyLevel, isolationLevel, lockAcquisitionTimeout, useLockStriping, writeSkewCheck);
   }

   @Override
   public LockingConfigurationBuilder read(LockingConfiguration template) {
      concurrencyLevel = template.concurrencyLevel();
      isolationLevel = template.isolationLevel();
      lockAcquisitionTimeout = template.lockAcquisitionTimeout();
      useLockStriping = template.useLockStriping();
      writeSkewCheck = template.writeSkewCheck();

      return this;
   }
}
