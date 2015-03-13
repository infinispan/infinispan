package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.util.concurrent.IsolationLevel;

/**
 * Defines the local, in-VM locking and concurrency characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class LockingConfiguration {
   static final AttributeDefinition<Integer> CONCURRENCY_LEVEL = AttributeDefinition.builder("concurrencyLevel", 32).immutable().build();
   static final AttributeDefinition<IsolationLevel> ISOLATION_LEVEL  = AttributeDefinition.builder("isolationLevel", IsolationLevel.READ_COMMITTED).immutable().build();
   static final AttributeDefinition<Long> LOCK_ACQUISITION_TIMEOUT  = AttributeDefinition.builder("lockAcquisitionTimeout", TimeUnit.SECONDS.toMillis(10)).build();
   static final AttributeDefinition<Boolean> USE_LOCK_STRIPING = AttributeDefinition.builder("useLockStriping", false).immutable().build();
   static final AttributeDefinition<Boolean> WRITE_SKEW_CHECK = AttributeDefinition.builder("writeSkewCheck", false).immutable().build();

   static final AttributeSet attributeSet() {
      return new AttributeSet(LockingConfiguration.class, CONCURRENCY_LEVEL, ISOLATION_LEVEL, LOCK_ACQUISITION_TIMEOUT, USE_LOCK_STRIPING, WRITE_SKEW_CHECK);
   }

   private final AttributeSet attributes;

   LockingConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
   }

   /**
    * Concurrency level for lock containers. Adjust this value according to the number of concurrent
    * threads interacting with Infinispan. Similar to the concurrencyLevel tuning parameter seen in
    * the JDK's ConcurrentHashMap.
    */
   public int concurrencyLevel() {
      return attributes.attribute(CONCURRENCY_LEVEL).asInteger();
   }

   /**
    * This option applies to non-transactional caches only (both clustered and local): if set to true(default value) the cache
    * keeps data consistent in the case of concurrent updates. For clustered caches this comes at the cost of an additional RPC, so if you don't expect your
    * application to write data concurrently, disabling this flag increases performance.
    *
    * @deprecated this option is always <code>true</code> and cannot be modified since version 5.3
    */
   @Deprecated
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
      return attributes.attribute(ISOLATION_LEVEL).asObject(IsolationLevel.class);
   }

   /**
    * Maximum time to attempt a particular lock acquisition
    */
   public long lockAcquisitionTimeout() {
      return attributes.attribute(LOCK_ACQUISITION_TIMEOUT).asLong();
   }

   public LockingConfiguration lockAcquisitionTimeout(long lockAcquisitionTimeout) {
      attributes.attribute(LOCK_ACQUISITION_TIMEOUT).set(lockAcquisitionTimeout);
      return this;
   }

   /**
    * If true, a pool of shared locks is maintained for all entries that need to be locked.
    * Otherwise, a lock is created per entry in the cache. Lock striping helps control memory
    * footprint but may reduce concurrency in the system.
    */
   public boolean useLockStriping() {
      return attributes.attribute(USE_LOCK_STRIPING).asBoolean();
   }

   /**
    * This setting is only applicable in the case of REPEATABLE_READ. When write skew check is set
    * to false, if the writer at commit time discovers that the working entry and the underlying
    * entry have different versions, the working entry will overwrite the underlying entry. If true,
    * such version conflict - known as a write-skew - will throw an Exception.
    */
   public boolean writeSkewCheck() {
      return attributes.attribute(WRITE_SKEW_CHECK).asBoolean();
   }

   AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "LockingConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      LockingConfiguration other = (LockingConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

}
