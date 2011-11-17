package org.infinispan.configuration.cache;

import org.infinispan.util.concurrent.IsolationLevel;

public class LockingConfiguration {
   
   private final int concurrencyLevel;
   private final IsolationLevel isolationLevel;
   private final long lockAcquisitionTimeout;
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

   public int getConcurrencyLevel() {
      return concurrencyLevel;
   }

   public IsolationLevel getIsolationLevel() {
      return isolationLevel;
   }

   public long getLockAcquisitionTimeout() {
      return lockAcquisitionTimeout;
   }

   public boolean isUseLockStriping() {
      return useLockStriping;
   }

   public boolean isWriteSkewCheck() {
      return writeSkewCheck;
   }

}
