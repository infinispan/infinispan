package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.Element.CONNECTION_POOL;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

@BuiltBy(ConnectionPoolConfigurationBuilder.class)
public class ConnectionPoolConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<ExhaustedAction> EXHAUSTED_ACTION = AttributeDefinition.builder("exhaustedAction", ExhaustedAction.WAIT, ExhaustedAction.class).immutable().build();
   static final AttributeDefinition<Integer> MAX_ACTIVE = AttributeDefinition.builder("maxActive", ConfigurationProperties.DEFAULT_MAX_ACTIVE).immutable().build();
   static final AttributeDefinition<Integer> MAX_WAIT = AttributeDefinition.builder("maxWait", ConfigurationProperties.DEFAULT_MAX_WAIT).immutable().build();
   static final AttributeDefinition<Integer> MIN_IDLE = AttributeDefinition.builder("minIdle", ConfigurationProperties.DEFAULT_MIN_IDLE).immutable().build();
   static final AttributeDefinition<Long> MIN_EVICTABLE_IDLE_TIME = AttributeDefinition.builder("minEvictableIdleTime", ConfigurationProperties.DEFAULT_MIN_EVICTABLE_IDLE_TIME).immutable().build();
   static final AttributeDefinition<Integer> MAX_PENDING_REQUESTS = AttributeDefinition.builder("maxPendingRequests", ConfigurationProperties.DEFAULT_MAX_PENDING_REQUESTS).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ConnectionPoolConfiguration.class, EXHAUSTED_ACTION, MAX_ACTIVE, MAX_WAIT, MIN_IDLE, MIN_EVICTABLE_IDLE_TIME, MAX_PENDING_REQUESTS);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(CONNECTION_POOL.getLocalName());

   private final AttributeSet attributes;

   ConnectionPoolConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }


   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public ExhaustedAction exhaustedAction() {
      return attributes.attribute(EXHAUSTED_ACTION).get();
   }

   public int maxActive() {
      return attributes.attribute(MAX_ACTIVE).get();
   }

   public int maxWait() {
      return attributes.attribute(MAX_WAIT).get();
   }

   /*
    * deprecated since 10.0. Always returns -1
    */
   @Deprecated
   public int maxIdle() {
      return -1;
   }

   public int minIdle() {
      return attributes.attribute(MIN_IDLE).get();
   }

   public int maxPendingRequests() {
      return attributes.attribute(MAX_PENDING_REQUESTS).get();
   }

   public long minEvictableIdleTime() {
      return attributes.attribute(MIN_EVICTABLE_IDLE_TIME).get();
   }

   @Override
   public String toString() {
      return "ConnectionPoolConfiguration [exhaustedAction=" + exhaustedAction() + ", maxActive=" + maxActive()
            + ", maxPendingRequests=" + maxPendingRequests() + ", maxWait=" + maxWait() + ", minIdle=" + minIdle() + ", minEvictableIdleTime=" + minEvictableIdleTime() + "]";
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ConnectionPoolConfiguration that = (ConnectionPoolConfiguration) o;

      if (maxActive() != that.maxActive()) return false;
      if (maxWait() != that.maxWait()) return false;
      if (minIdle() != that.minIdle()) return false;
      if (minEvictableIdleTime() != that.minEvictableIdleTime()) return false;
      if (maxPendingRequests() != that.maxPendingRequests()) return false;
      return exhaustedAction() == that.exhaustedAction();

   }

   @Override
   public int hashCode() {
      int result = exhaustedAction() != null ? exhaustedAction().hashCode() : 0;
      result = 31 * result + maxActive();
      result = 31 * result + maxWait();
      result = 31 * result + minIdle();
      result = 31 * result + (int) (minEvictableIdleTime() ^ (minEvictableIdleTime() >>> 32));
      result = 31 * result + maxPendingRequests();
      return result;
   }
}
