package org.infinispan.hotrod.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.hotrod.near.DefaultNearCacheFactory;

public class NearCacheConfiguration extends ConfigurationElement<NearCacheConfiguration> {
   // TODO: Consider an option to configure key equivalence function for near cache (e.g. for byte arrays)
   static final AttributeDefinition<NearCacheMode> MODE = AttributeDefinition.builder("mode", NearCacheMode.DISABLED, NearCacheMode.class).build();
   static final AttributeDefinition<Integer> MAX_ENTRIES = AttributeDefinition.builder("max-entries", null, Integer.class).build();
   static final AttributeDefinition<Boolean> BLOOM_FILTER = AttributeDefinition.builder("bloom-filter", false, Boolean.class).build();
   static final AttributeDefinition<NearCacheFactory> FACTORY = AttributeDefinition.builder("factory", DefaultNearCacheFactory.INSTANCE, NearCacheFactory.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(ServerConfiguration.class, MODE, MAX_ENTRIES, BLOOM_FILTER, FACTORY);
   }

   NearCacheConfiguration(AttributeSet attributes) {
      super("near-cache", attributes);
   }

   public int maxEntries() {
      return attributes.attribute(MAX_ENTRIES).get();
   }

   public NearCacheMode mode() {
      return attributes.attribute(MODE).get();
   }

   public boolean bloomFilter() {
      return attributes.attribute(BLOOM_FILTER).get();
   }

   public NearCacheFactory nearCacheFactory() {
      return attributes.attribute(FACTORY).get();
   }
}
