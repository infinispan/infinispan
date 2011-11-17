package org.infinispan.configuration.cache;

import org.infinispan.util.concurrent.IsolationLevel;

public class LockingConfigurationBuilder extends AbstractConfigurationChildBuilder<LockingConfiguration> {

   private int concurrencyLevel;
   private IsolationLevel isolationLevel;
   private long lockAcquisitionTimeout;
   private boolean useLockStriping;
   private boolean writeSkewCheck;

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
