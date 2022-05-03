package org.infinispan.hotrod.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * ConnectionPoolConfiguration.
 *
 * @since 14.0
 */
public class ConnectionPoolConfiguration extends ConfigurationElement<ConnectionPoolConfiguration> {
   static final AttributeDefinition<ExhaustedAction> EXHAUSTED_ACTION = AttributeDefinition.builder("exhausted_action", ExhaustedAction.WAIT, ExhaustedAction.class).build();
   static final AttributeDefinition<Integer> MAX_ACTIVE = AttributeDefinition.builder("max_active", -1, Integer.class).build();
   static final AttributeDefinition<Long> MAX_WAIT = AttributeDefinition.builder("max_wait", -1l, Long.class).build();
   static final AttributeDefinition<Integer> MIN_IDLE = AttributeDefinition.builder("min_idle", -1, Integer.class).build();
   static final AttributeDefinition<Long> MIN_EVICTABLE_IDLE_TIME = AttributeDefinition.builder("min_evictable_idle_time", 1800000L, Long.class).build();
   static final AttributeDefinition<Integer> MAX_PENDING_REQUESTS = AttributeDefinition.builder("max_pending_requests", 5, Integer.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ServerConfiguration.class, EXHAUSTED_ACTION, MAX_ACTIVE, MAX_WAIT, MIN_IDLE, MIN_EVICTABLE_IDLE_TIME, MAX_PENDING_REQUESTS);
   }

   ConnectionPoolConfiguration(AttributeSet attributes) {
      super("connection-pool", attributes);
   }

   public ExhaustedAction exhaustedAction() {
      return attributes.attribute(EXHAUSTED_ACTION).get();
   }

   public int maxActive() {
      return attributes.attribute(MAX_ACTIVE).get();
   }

   public long maxWait() {
      return attributes.attribute(MAX_WAIT).get();
   }

   public int minIdle() {
      return attributes.attribute(MIN_IDLE).get();
   }

   public long minEvictableIdleTime() {
      return attributes.attribute(MIN_EVICTABLE_IDLE_TIME).get();
   }

   public int maxPendingRequests() {
      return attributes.attribute(MAX_PENDING_REQUESTS).get();
   }
}
