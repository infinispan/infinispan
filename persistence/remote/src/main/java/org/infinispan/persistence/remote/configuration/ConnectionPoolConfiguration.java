package org.infinispan.persistence.remote.configuration;

import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

@BuiltBy(ConnectionPoolConfigurationBuilder.class)
public class ConnectionPoolConfiguration {

   static final AttributeDefinition<ExhaustedAction> EXHAUSTED_ACTION = AttributeDefinition.builder(Attribute.EXHAUSTED_ACTION, ExhaustedAction.WAIT, ExhaustedAction.class).immutable().build();
   static final AttributeDefinition<Integer> MAX_ACTIVE = AttributeDefinition.builder(Attribute.MAX_ACTIVE, ConfigurationProperties.DEFAULT_MAX_ACTIVE).immutable().build();
   static final AttributeDefinition<Integer> MAX_WAIT = AttributeDefinition.builder(Attribute.MAX_WAIT, ConfigurationProperties.DEFAULT_MAX_WAIT).immutable().build();
   static final AttributeDefinition<Integer> MIN_IDLE = AttributeDefinition.builder(Attribute.MIN_IDLE, ConfigurationProperties.DEFAULT_MIN_IDLE).immutable().build();
   static final AttributeDefinition<Long> MIN_EVICTABLE_IDLE_TIME = AttributeDefinition.builder(Attribute.MIN_EVICTABLE_IDLE_TIME, ConfigurationProperties.DEFAULT_MIN_EVICTABLE_IDLE_TIME).immutable().build();
   static final AttributeDefinition<Integer> MAX_PENDING_REQUESTS = AttributeDefinition.builder(Attribute.MAX_PENDING_REQUESTS, ConfigurationProperties.DEFAULT_MAX_PENDING_REQUESTS).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ConnectionPoolConfiguration.class, EXHAUSTED_ACTION, MAX_ACTIVE, MAX_WAIT, MIN_IDLE, MIN_EVICTABLE_IDLE_TIME, MAX_PENDING_REQUESTS);
   }

   private final AttributeSet attributes;

   ConnectionPoolConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
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
