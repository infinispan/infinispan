package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.TimeUnit;

/**
 * Defines the local, in-VM locking and concurrency characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class LockingConfigurationBuilder extends AbstractConfigurationChildBuilder implements Builder<LockingConfiguration> {

   private static final Log log = LogFactory.getLog(LockingConfigurationBuilder.class);

   private int concurrencyLevel = 32;
   private IsolationLevel isolationLevel;
   private long lockAcquisitionTimeout = TimeUnit.SECONDS.toMillis(10);
   private boolean useLockStriping = false;
   private Boolean writeSkewCheck;

   protected LockingConfigurationBuilder(ConfigurationBuilder builder) {
      super(builder);
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
    * @see org.infinispan.configuration.cache.LockingConfiguration#supportsConcurrentUpdates()
    * @deprecated
    */
   public LockingConfigurationBuilder supportsConcurrentUpdates(boolean itDoes) {
      if (!itDoes) {
         log.warnConcurrentUpdateSupportCannotBeConfigured();
      }
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
    * Maximum time to attempt a particular lock acquisition
    */
   public LockingConfigurationBuilder lockAcquisitionTimeout(long l, TimeUnit unit) {
      return lockAcquisitionTimeout(unit.toMillis(l));
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
   public void validate() {
      if (writeSkewCheck != null && writeSkewCheck) {
         if (isolationLevel != IsolationLevel.REPEATABLE_READ)
            throw new CacheConfigurationException("Write-skew checking only allowed with REPEATABLE_READ isolation level for cache");
         if (transaction().lockingMode != LockingMode.OPTIMISTIC)
            throw new CacheConfigurationException("Write-skew checking only allowed with OPTIMISTIC transactions");
         if ((versioning().enabled == null || ! versioning().enabled)
               || (versioning().scheme == null || versioning().scheme != VersioningScheme.SIMPLE))
            throw new CacheConfigurationException(
                  "Write-skew checking requires versioning to be enabled and versioning scheme 'SIMPLE' to be configured");
         if (clustering().cacheMode() != CacheMode.DIST_SYNC && clustering().cacheMode() != CacheMode.REPL_SYNC
               && clustering().cacheMode() != CacheMode.LOCAL)
            throw new CacheConfigurationException("Write-skew checking is only supported in REPL_SYNC, DIST_SYNC and LOCAL modes.  "
                  + clustering().cacheMode() + " cannot be used with write-skew checking");
      }
   }

   @Override
   public LockingConfiguration create() {
      CacheMode cacheMode = getBuilder().clustering().cacheMode();
      if (cacheMode.isClustered() && isolationLevel == IsolationLevel.NONE)
         isolationLevel = IsolationLevel.READ_COMMITTED;

      if (isolationLevel == IsolationLevel.READ_UNCOMMITTED)
         isolationLevel = IsolationLevel.READ_COMMITTED;

      if (isolationLevel == IsolationLevel.SERIALIZABLE)
         isolationLevel = IsolationLevel.REPEATABLE_READ;

      if (writeSkewCheck == null)
         writeSkewCheck = Configurations.isStrictOptimisticTransaction(getBuilder());

      if (isolationLevel == null)
         isolationLevel = Configurations.isStrictOptimisticTransaction(getBuilder())
               ? IsolationLevel.REPEATABLE_READ
               : IsolationLevel.READ_COMMITTED;
      else if (isolationLevel == IsolationLevel.READ_COMMITTED)
         writeSkewCheck = false;

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

   @Override
   public String toString() {
      return "LockingConfigurationBuilder{" +
            "concurrencyLevel=" + concurrencyLevel +
            ", isolationLevel=" + isolationLevel +
            ", lockAcquisitionTimeout=" + lockAcquisitionTimeout +
            ", useLockStriping=" + useLockStriping +
            ", writeSkewCheck=" + writeSkewCheck +
            '}';
   }

}
