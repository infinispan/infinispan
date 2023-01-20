package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.util.concurrent.IsolationLevel;

/**
 * Defines the local, in-VM locking and concurrency characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class LockingConfiguration extends ConfigurationElement<LockingConfiguration> {
   public static final AttributeDefinition<Integer> CONCURRENCY_LEVEL = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.CONCURRENCY_LEVEL, 32).immutable().build();
   public static final AttributeDefinition<IsolationLevel> ISOLATION_LEVEL  = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ISOLATION, IsolationLevel.REPEATABLE_READ).immutable().build();
   public static final AttributeDefinition<Long> LOCK_ACQUISITION_TIMEOUT  = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.ACQUIRE_TIMEOUT, TimeUnit.SECONDS.toMillis(10)).build();
   public static final AttributeDefinition<Boolean> USE_LOCK_STRIPING = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.STRIPING, false).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LockingConfiguration.class, Element.LOCKING.toString(), null,
            new AttributeDefinition[]{
                  CONCURRENCY_LEVEL, ISOLATION_LEVEL, LOCK_ACQUISITION_TIMEOUT, USE_LOCK_STRIPING
            },
            new AttributeSet.RemovedAttribute[] { new AttributeSet.RemovedAttribute(org.infinispan.configuration.parsing.Attribute.WRITE_SKEW_CHECK, 10, 0)}
      );
   }

   private final Attribute<Integer> concurrencyLevel;
   private final Attribute<IsolationLevel> isolationLevel;
   private final Attribute<Long> lockAcquisitionTimeout;
   private final Attribute<Boolean> useLockStriping;

   LockingConfiguration(AttributeSet attributes) {
      super(Element.LOCKING, attributes);
      concurrencyLevel = attributes.attribute(CONCURRENCY_LEVEL);
      isolationLevel = attributes.attribute(ISOLATION_LEVEL);
      lockAcquisitionTimeout = attributes.attribute(LOCK_ACQUISITION_TIMEOUT);
      useLockStriping = attributes.attribute(USE_LOCK_STRIPING);
   }

   /**
    * Concurrency level for lock containers. Adjust this value according to the number of concurrent
    * threads interacting with Infinispan. Similar to the concurrencyLevel tuning parameter seen in
    * the JDK's ConcurrentHashMap.
    */
   public int concurrencyLevel() {
      return concurrencyLevel.get();
   }

   /**
    * Cache isolation level. Infinispan only supports READ_COMMITTED or REPEATABLE_READ isolation
    * levels. See <a href=
    * 'http://en.wikipedia.org/wiki/Isolation_level'>http://en.wikipedia.org/wiki/Isolation_level</a
    * > for a discussion on isolation levels.
    */
   public IsolationLevel isolationLevel() {
      return isolationLevel.get();
   }

   /**
    * Maximum time to attempt a particular lock acquisition
    */
   public long lockAcquisitionTimeout() {
      return lockAcquisitionTimeout.get();
   }

   public LockingConfiguration lockAcquisitionTimeout(long timeout) {
      lockAcquisitionTimeout.set(timeout);
      return this;
   }

   /**
    * If true, a pool of shared locks is maintained for all entries that need to be locked.
    * Otherwise, a lock is created per entry in the cache. Lock striping helps control memory
    * footprint but may reduce concurrency in the system.
    */
   public boolean useLockStriping() {
      return useLockStriping.get();
   }
}
