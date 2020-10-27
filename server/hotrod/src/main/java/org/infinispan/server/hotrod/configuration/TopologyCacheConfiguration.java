package org.infinispan.server.hotrod.configuration;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
public class TopologyCacheConfiguration implements ConfigurationInfo {
   public static final AttributeDefinition<Boolean> TOPOLOGY_AWAIT_INITIAL_TRANSFER = AttributeDefinition.builder("awaitInitialRetrieval", true).immutable().build();
   public static final AttributeDefinition<Long> TOPOLOGY_LOCK_TIMEOUT = AttributeDefinition.builder("lockTimeout", 10000L).immutable().build();
   public static final AttributeDefinition<Long> TOPOLOGY_REPL_TIMEOUT = AttributeDefinition.builder("replicationTimeout", 10000L).immutable().build();
   public static final AttributeDefinition<Boolean> LAZY_RETRIEVAL = AttributeDefinition.builder("lazyRetrieval", false).immutable().build();

   private final Attribute<Long> lockTimeout;
   private final Attribute<Long> replicationTimeout;
   private final Attribute<Boolean> awaitInitialTransfer;
   private final Attribute<Boolean> lazyRetrieval;
   private final AttributeSet attributes;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(TopologyCacheConfiguration.class, TOPOLOGY_AWAIT_INITIAL_TRANSFER, TOPOLOGY_LOCK_TIMEOUT, TOPOLOGY_REPL_TIMEOUT, LAZY_RETRIEVAL);
   }

   public static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("topology-state-transfer");

   TopologyCacheConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
      lockTimeout = attributes.attribute(TOPOLOGY_LOCK_TIMEOUT);
      replicationTimeout = attributes.attribute(TOPOLOGY_REPL_TIMEOUT);
      awaitInitialTransfer = attributes.attribute(TOPOLOGY_AWAIT_INITIAL_TRANSFER);
      lazyRetrieval = attributes.attribute(LAZY_RETRIEVAL);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public long lockTimeout() {
      return lockTimeout.get();
   }

   public long replicationTimeout() {
      return replicationTimeout.get();
   }

   public boolean awaitInitialTransfer() {
      return awaitInitialTransfer.get();
   }

   public boolean lazyRetrieval() {
      return lazyRetrieval.get();
   }

   @Override
   public String toString() {
      return "TopologyCacheConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
