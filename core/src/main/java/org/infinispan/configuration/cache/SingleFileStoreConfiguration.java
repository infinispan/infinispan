package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.file.SingleFileStore;

/**
 * Defines the configuration for the single file cache store.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@BuiltBy(SingleFileStoreConfigurationBuilder.class)
@ConfigurationFor(SingleFileStore.class)
public class SingleFileStoreConfiguration extends AbstractStoreConfiguration {
   static final AttributeDefinition<String> LOCATION = AttributeDefinition.builder("location", "Infinispan-SingleFileStore").immutable().build();
   static final AttributeDefinition<Integer> MAX_ENTRIES = AttributeDefinition.builder("maxEntries", -1).immutable().build();
   static final AttributeDefinition<Float> FRAGMENTATION_FACTOR = AttributeDefinition.builder("fragmentationFactor", 0.75f).immutable().build();
   public static AttributeSet attributeSet() {
      return new AttributeSet(SingleFileStoreConfiguration.class, AbstractStoreConfiguration.attributeSet(), LOCATION, MAX_ENTRIES, FRAGMENTATION_FACTOR);
   }

   public SingleFileStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
                                       SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
   }

   public String location() {
      return attributes.attribute(LOCATION).asString();
   }

   public int maxEntries() {
      return attributes.attribute(MAX_ENTRIES).asInteger();
   }

   public float fragmentationFactor () {
      return attributes.attribute(FRAGMENTATION_FACTOR).asFloat();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "SingleFileStoreConfiguration [attributes=" + attributes + "]";
   }
}
