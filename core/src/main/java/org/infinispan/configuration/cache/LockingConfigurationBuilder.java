package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.util.concurrent.IsolationLevel;

public class LockingConfigurationBuilder extends AbstractConfigurationChildBuilder<LockingConfiguration> {

   private int concurrencyLevel = 32;
   private IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
   private long lockAcquisitionTimeout = TimeUnit.SECONDS.toMillis(10);
   private boolean useLockStriping = false;
   private boolean writeSkewCheck = false;

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
      // TODO Auto-generated method stub

   }

   @Override
   LockingConfiguration create() {
      return new LockingConfiguration(concurrencyLevel, isolationLevel, lockAcquisitionTimeout, useLockStriping, writeSkewCheck);
   }
}
