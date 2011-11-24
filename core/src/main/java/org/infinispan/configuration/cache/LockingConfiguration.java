package org.infinispan.configuration.cache;

import org.infinispan.util.concurrent.IsolationLevel;

public class LockingConfiguration {
   
   private final int concurrencyLevel;
   private final IsolationLevel isolationLevel;
   private long lockAcquisitionTimeout;
   private final boolean useLockStriping;
   private final boolean writeSkewCheck;
   
   LockingConfiguration(int concurrencyLevel, IsolationLevel isolationLevel, long lockAcquisitionTimeout,
         boolean useLockStriping, boolean writeSkewCheck) {
      this.concurrencyLevel = concurrencyLevel;
      this.isolationLevel = isolationLevel;
      this.lockAcquisitionTimeout = lockAcquisitionTimeout;
      this.useLockStriping = useLockStriping;
      this.writeSkewCheck = writeSkewCheck;
   }

   public int concurrencyLevel() {
      return concurrencyLevel;
   }

   public IsolationLevel isolationLevel() {
      return isolationLevel;
   }

   public long lockAcquisitionTimeout() {
      return lockAcquisitionTimeout;
   }
   
   public LockingConfiguration lockAcquisitionTimeout(long lockAcquisitionTimeout) {
      this.lockAcquisitionTimeout = lockAcquisitionTimeout;
      return this;
   }

   public boolean useLockStriping() {
      return useLockStriping;
   }

   public boolean writeSkewCheck() {
      return writeSkewCheck;
   }

}
