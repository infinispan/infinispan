/*
 * Copyright 2011 Red Hat, Inc. and/or its affiliates.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
package org.infinispan.configuration.cache;

import org.infinispan.util.concurrent.IsolationLevel;

/**
 * Defines the local, in-VM locking and concurrency characteristics of the cache.
 * 
 * @author pmuir
 * 
 */
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

   /**
    * Concurrency level for lock containers. Adjust this value according to the number of concurrent
    * threads interacting with Infinispan. Similar to the concurrencyLevel tuning parameter seen in
    * the JDK's ConcurrentHashMap.
    */
   public int concurrencyLevel() {
      return concurrencyLevel;
   }

   /**
    * This option applies to non-transactional caches only (both clustered and local): if set to true(default value) the cache
    * keeps data consistent in the case of concurrent updates. For clustered caches this comes at the cost of an additional RPC, so if you don't expect your
    * application to write data concurrently, disabling this flag increases performance.
    *
    * @deprecated this option is always <code>true</code> and cannot be modified since version 5.3
    */
   public boolean supportsConcurrentUpdates() {
      return true;
   }

   /**
    * Cache isolation level. Infinispan only supports READ_COMMITTED or REPEATABLE_READ isolation
    * levels. See <a href=
    * 'http://en.wikipedia.org/wiki/Isolation_level'>http://en.wikipedia.org/wiki/Isolation_level</a
    * > for a discussion on isolation levels.
    */
   public IsolationLevel isolationLevel() {
      return isolationLevel;
   }

   /**
    * Maximum time to attempt a particular lock acquisition
    */
   public long lockAcquisitionTimeout() {
      return lockAcquisitionTimeout;
   }

   public LockingConfiguration lockAcquisitionTimeout(long lockAcquisitionTimeout) {
      this.lockAcquisitionTimeout = lockAcquisitionTimeout;
      return this;
   }

   /**
    * If true, a pool of shared locks is maintained for all entries that need to be locked.
    * Otherwise, a lock is created per entry in the cache. Lock striping helps control memory
    * footprint but may reduce concurrency in the system.
    */
   public boolean useLockStriping() {
      return useLockStriping;
   }

   /**
    * This setting is only applicable in the case of REPEATABLE_READ. When write skew check is set
    * to false, if the writer at commit time discovers that the working entry and the underlying
    * entry have different versions, the working entry will overwrite the underlying entry. If true,
    * such version conflict - known as a write-skew - will throw an Exception.
    */
   public boolean writeSkewCheck() {
      return writeSkewCheck;
   }

   @Override
   public String toString() {
      return "LockingConfiguration{" +
            "concurrencyLevel=" + concurrencyLevel +
            ", isolationLevel=" + isolationLevel +
            ", lockAcquisitionTimeout=" + lockAcquisitionTimeout +
            ", useLockStriping=" + useLockStriping +
            ", writeSkewCheck=" + writeSkewCheck +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      LockingConfiguration that = (LockingConfiguration) o;

      if (concurrencyLevel != that.concurrencyLevel) return false;
      if (lockAcquisitionTimeout != that.lockAcquisitionTimeout) return false;
      if (useLockStriping != that.useLockStriping) return false;
      if (writeSkewCheck != that.writeSkewCheck) return false;
      if (isolationLevel != that.isolationLevel) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = concurrencyLevel;
      result = 31 * result + (isolationLevel != null ? isolationLevel.hashCode() : 0);
      result = 31 * result + (int) (lockAcquisitionTimeout ^ (lockAcquisitionTimeout >>> 32));
      result = 31 * result + (useLockStriping ? 1 : 0);
      result = 31 * result + (writeSkewCheck ? 1 : 0);
      return result;
   }

}
