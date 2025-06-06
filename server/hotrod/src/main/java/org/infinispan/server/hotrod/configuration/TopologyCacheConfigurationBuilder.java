package org.infinispan.server.hotrod.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.TimeQuantity;

public class TopologyCacheConfigurationBuilder implements Builder<TopologyCacheConfiguration> {
   private final AttributeSet attributes;

   TopologyCacheConfigurationBuilder() {
      this.attributes = TopologyCacheConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public TopologyCacheConfigurationBuilder lockTimeout(long value) {
      attributes.attribute(TopologyCacheConfiguration.TOPOLOGY_LOCK_TIMEOUT).set(TimeQuantity.valueOf(value));
      return this;
   }

   public TopologyCacheConfigurationBuilder lockTimeout(String value) {
      attributes.attribute(TopologyCacheConfiguration.TOPOLOGY_LOCK_TIMEOUT).set(TimeQuantity.valueOf(value));
      return this;
   }

   public TopologyCacheConfigurationBuilder replicationTimeout(long value) {
      attributes.attribute(TopologyCacheConfiguration.TOPOLOGY_REPL_TIMEOUT).set(TimeQuantity.valueOf(value));
      return this;
   }

   public TopologyCacheConfigurationBuilder replicationTimeout(String value) {
      attributes.attribute(TopologyCacheConfiguration.TOPOLOGY_REPL_TIMEOUT).set(TimeQuantity.valueOf(value));
      return this;
   }

   public TopologyCacheConfigurationBuilder awaitInitialTransfer(boolean await) {
      attributes.attribute(TopologyCacheConfiguration.TOPOLOGY_AWAIT_INITIAL_TRANSFER).set(await);
      return this;
   }

   public TopologyCacheConfigurationBuilder networkPrefixOverride(boolean networkPrefixOverride) {
      attributes.attribute(TopologyCacheConfiguration.NETWORK_PREFIX_OVERRIDE).set(networkPrefixOverride);
      return this;
   }

   @Override
   public TopologyCacheConfiguration create() {
      return new TopologyCacheConfiguration(attributes.protect());
   }

   @Override
   public TopologyCacheConfigurationBuilder read(TopologyCacheConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }
}
