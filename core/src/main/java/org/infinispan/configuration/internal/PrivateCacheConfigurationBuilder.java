package org.infinispan.configuration.internal;

import static org.infinispan.configuration.internal.PrivateCacheConfiguration.CONSISTENT_HASH_FACTORY;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.ConsistentHashFactory;

public class PrivateCacheConfigurationBuilder implements Builder<PrivateCacheConfiguration> {

   private final AttributeSet attributes;

   public PrivateCacheConfigurationBuilder(ConfigurationBuilder builder) {
      this.attributes = PrivateCacheConfiguration.attributeSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public PrivateCacheConfigurationBuilder consistentHashFactory(ConsistentHashFactory<? extends ConsistentHash> consistentHashFactory) {
      attributes.attribute(CONSISTENT_HASH_FACTORY).set(consistentHashFactory);
      return this;
   }

   @Override
   public void validate() {
      //nothing
   }

   @Override
   public PrivateCacheConfiguration create() {
      return new PrivateCacheConfiguration(attributes.protect());
   }

   @Override
   public Builder<?> read(PrivateCacheConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }

   @Override
   public String toString() {
      return "PrivateCacheConfiguration [attributes=" + attributes + ']';
   }
}
