package org.infinispan.configuration.cache;

import org.infinispan.config.ConfigurationException;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.concurrent.IsolationLevel;

import java.util.concurrent.TimeUnit;

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

   public LockingConfigurationBuilder concurrencyLevel(int i) {
      this.concurrencyLevel = i;
      return this;
   }

   public LockingConfigurationBuilder isolationLevel(IsolationLevel isolationLevel) {
      this.isolationLevel = isolationLevel;
      return this;
   }

   public LockingConfigurationBuilder lockAcquisitionTimeout(long l) {
      this.lockAcquisitionTimeout = l;
      return this;
   }

   public LockingConfigurationBuilder useLockStriping(boolean b) {
      this.useLockStriping = b;
      return this;
   }

   public LockingConfigurationBuilder writeSkewCheck(boolean b) {
      this.writeSkewCheck = b;
      return this;
   }

   @Override
   void validate() {
      if (writeSkewCheck) {
         if (isolationLevel != IsolationLevel.REPEATABLE_READ) throw new ConfigurationException("Write-skew checking only allowed with REPEATABLE_READ isolation level for cache " + getBuilder().name);
         if (transaction().lockingMode != LockingMode.OPTIMISTIC) throw new ConfigurationException("Write-skew checking only allowed with OPTIMISTIC transactions");
         if (!versioning().enabled || versioning().scheme != VersioningScheme.SIMPLE)
            throw new ConfigurationException("Write-skew checking requires versioning to be enabled and versioning scheme 'SIMPLE' to be configured");
         if (clustering().cacheMode() != CacheMode.DIST_SYNC && clustering().cacheMode() != CacheMode.REPL_SYNC &&
               clustering().cacheMode() != CacheMode.LOCAL)
            throw new ConfigurationException("Write-skew checking is only supported in REPL_SYNC, DIST_SYNC and LOCAL modes.  " + clustering().cacheMode() + " cannot be used with write-skew checking");
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
