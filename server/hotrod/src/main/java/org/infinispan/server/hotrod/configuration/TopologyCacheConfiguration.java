package org.infinispan.server.hotrod.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.TimeQuantity;

/**
 * @since 10.0
 */
public class TopologyCacheConfiguration extends ConfigurationElement<TopologyCacheConfiguration> {
   public static final AttributeDefinition<Boolean> TOPOLOGY_AWAIT_INITIAL_TRANSFER = AttributeDefinition.builder(Attribute.AWAIT_INITIAL_RETRIEVAL, true).immutable().build();
   public static final AttributeDefinition<TimeQuantity> TOPOLOGY_LOCK_TIMEOUT = AttributeDefinition.builder(Attribute.LOCK_TIMEOUT, TimeQuantity.valueOf("10s")).immutable().build();
   public static final AttributeDefinition<TimeQuantity> TOPOLOGY_REPL_TIMEOUT = AttributeDefinition.builder(Attribute.REPLICATION_TIMEOUT, TimeQuantity.valueOf("10s")).immutable().build();
   public static final AttributeDefinition<Boolean> NETWORK_PREFIX_OVERRIDE = AttributeDefinition.builder(Attribute.NETWORK_PREFIX_OVERRIDE, true).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TopologyCacheConfiguration.class, TOPOLOGY_AWAIT_INITIAL_TRANSFER, TOPOLOGY_LOCK_TIMEOUT, TOPOLOGY_REPL_TIMEOUT, NETWORK_PREFIX_OVERRIDE);
   }

   TopologyCacheConfiguration(AttributeSet attributes) {
      super(Element.TOPOLOGY_STATE_TRANSFER, attributes);
   }

   public long lockTimeout() {
      return attributes.attribute(TOPOLOGY_LOCK_TIMEOUT).get().longValue();
   }

   public long replicationTimeout() {
      return attributes.attribute(TOPOLOGY_REPL_TIMEOUT).get().longValue();
   }

   public boolean awaitInitialTransfer() {
      return attributes.attribute(TOPOLOGY_AWAIT_INITIAL_TRANSFER).get();
   }

   public boolean networkPrefixOverride() {
      return attributes.attribute(NETWORK_PREFIX_OVERRIDE).get();
   }
}
