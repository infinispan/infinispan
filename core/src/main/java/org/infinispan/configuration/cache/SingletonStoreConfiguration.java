package org.infinispan.configuration.cache;

import java.util.concurrent.TimeUnit;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.Matchable;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * SingletonStore is a delegating cache store used for situations when only one instance in a
 * cluster should interact with the underlying store. The coordinator of the cluster will be
 * responsible for the underlying CacheStore. SingletonStore is a simply facade to a real CacheStore
 * implementation. It always delegates reads to the real CacheStore.
 *
 * @author pmuir
 * @deprecated Singleton writers will be removed in 10.0. If it is desirable that all nodes don't write to the underlying store
 * then a shared store should be used instead, as this only performs store writes at a key's primary owner.
 */
@Deprecated
public class SingletonStoreConfiguration implements Matchable<SingletonStoreConfiguration>, ConfigurationInfo {
   public final static AttributeDefinition<Boolean> ENABLED = AttributeDefinition.builder("enabled", false).xmlName("singleton").immutable().build();
   public final static AttributeDefinition<Long> PUSH_STATE_TIMEOUT = AttributeDefinition.builder("push-state-timeout", TimeUnit.SECONDS.toMillis(10)).immutable().build();
   public final static AttributeDefinition<Boolean> PUSH_STATE_WHEN_COORDINATOR = AttributeDefinition.builder("push-state-when-coordinator", true).immutable().build();
   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SingletonStoreConfiguration.class, ENABLED, PUSH_STATE_TIMEOUT, PUSH_STATE_WHEN_COORDINATOR);
   }

   public static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("", false);

   private final Attribute<Boolean> enabled;
   private final Attribute<Long> pushStateTimeout;
   private final Attribute<Boolean> pushStateWhenCoordinator;
   private final AttributeSet attributes;

   SingletonStoreConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      enabled = attributes.attribute(ENABLED);
      pushStateTimeout = attributes.attribute(PUSH_STATE_TIMEOUT);
      pushStateWhenCoordinator = attributes.attribute(PUSH_STATE_WHEN_COORDINATOR);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   /**
    * If true, the singleton store cache store is enabled.
    */
   public boolean enabled() {
      return enabled.get();
   }

   /**
    * If pushStateWhenCoordinator is true, this property sets the maximum number of milliseconds
    * that the process of pushing the in-memory state to the underlying cache loader should take.
    */
   public long pushStateTimeout() {
      return pushStateTimeout.get();
   }

   /**
    * If true, when a node becomes the coordinator, it will transfer in-memory state to the
    * underlying cache store. This can be very useful in situations where the coordinator crashes
    * and there's a gap in time until the new coordinator is elected.
    */
   public boolean pushStateWhenCoordinator() {
      return pushStateWhenCoordinator.get();
   }

   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "SingletonStoreConfiguration [attributes=" + attributes + "]";
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((attributes == null) ? 0 : attributes.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      SingletonStoreConfiguration other = (SingletonStoreConfiguration) obj;
      if (attributes == null) {
         if (other.attributes != null)
            return false;
      } else if (!attributes.equals(other.attributes))
         return false;
      return true;
   }

}
