package org.infinispan.server.hotrod.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

/**
 * @since 10.0
 */
public class TopologyCacheConfiguration extends ConfigurationElement<TopologyCacheConfiguration> {
   public static final AttributeDefinition<Boolean> TOPOLOGY_AWAIT_INITIAL_TRANSFER = AttributeDefinition.builder(Attribute.AWAIT_INITIAL_RETRIEVAL, true).immutable().build();
   public static final AttributeDefinition<Long> TOPOLOGY_LOCK_TIMEOUT = AttributeDefinition.builder(Attribute.LOCK_TIMEOUT, 10000L).immutable().build();
   public static final AttributeDefinition<Long> TOPOLOGY_REPL_TIMEOUT = AttributeDefinition.builder(Attribute.REPLICATION_TIMEOUT, 10000L).immutable().build();
   public static final AttributeDefinition<Boolean> LAZY_RETRIEVAL = AttributeDefinition.builder(Attribute.LAZY_RETRIEVAL, false).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TopologyCacheConfiguration.class, TOPOLOGY_AWAIT_INITIAL_TRANSFER, TOPOLOGY_LOCK_TIMEOUT, TOPOLOGY_REPL_TIMEOUT, LAZY_RETRIEVAL);
   }

   TopologyCacheConfiguration(AttributeSet attributes) {
      super(Element.TOPOLOGY_STATE_TRANSFER, attributes);
   }

   public long lockTimeout() {
      return attributes.attribute(TOPOLOGY_LOCK_TIMEOUT).get();
   }

   public long replicationTimeout() {
      return attributes.attribute(TOPOLOGY_REPL_TIMEOUT).get();
   }

   public boolean awaitInitialTransfer() {
      return attributes.attribute(TOPOLOGY_AWAIT_INITIAL_TRANSFER).get();
   }

   public boolean lazyRetrieval() {
      return attributes.attribute(LAZY_RETRIEVAL).get();
   }
}
