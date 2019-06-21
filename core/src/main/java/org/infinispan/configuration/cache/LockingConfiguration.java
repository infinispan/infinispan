package org.infinispan.configuration.cache;

import static org.infinispan.configuration.parsing.Element.LOCKING;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.util.concurrent.IsolationLevel;

/**
 * Defines the local, in-VM locking and concurrency characteristics of the cache.
 *
 * @author pmuir
 *
 */
public class LockingConfiguration implements Matchable<LockingConfiguration>, ConfigurationInfo {
   public static final AttributeDefinition<Integer> CONCURRENCY_LEVEL = AttributeDefinition.builder("concurrencyLevel", 32).immutable().build();
   public static final AttributeDefinition<IsolationLevel> ISOLATION_LEVEL  = AttributeDefinition.builder("isolationLevel", IsolationLevel.REPEATABLE_READ).xmlName("isolation").immutable().build();
   public static final AttributeDefinition<Long> LOCK_ACQUISITION_TIMEOUT  = AttributeDefinition.builder("lockAcquisitionTimeout", TimeUnit.SECONDS.toMillis(10)).xmlName("acquire-timeout").build();
   public static final AttributeDefinition<Boolean> USE_LOCK_STRIPING = AttributeDefinition.builder("striping", false).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LockingConfiguration.class, CONCURRENCY_LEVEL, ISOLATION_LEVEL, LOCK_ACQUISITION_TIMEOUT, USE_LOCK_STRIPING);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(LOCKING.getLocalName());

   private final Attribute<Integer> concurrencyLevel;
   private final Attribute<IsolationLevel> isolationLevel;
   private final Attribute<Long> lockAcquisitionTimeout;
   private final Attribute<Boolean> useLockStriping;

   private final AttributeSet attributes;

   LockingConfiguration(AttributeSet attributes) {
      attributes.checkProtection();
      this.attributes = attributes;
      concurrencyLevel = attributes.attribute(CONCURRENCY_LEVEL);
      isolationLevel = attributes.attribute(ISOLATION_LEVEL);
      lockAcquisitionTimeout = attributes.attribute(LOCK_ACQUISITION_TIMEOUT);
      useLockStriping = attributes.attribute(USE_LOCK_STRIPING);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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

   public AttributeSet attributes() {
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
