package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.Attribute;
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
   public static final AttributeDefinition<String> LOCATION = AttributeDefinition.builder("location", "Infinispan-SingleFileStore").immutable().build();
   public static final AttributeDefinition<Integer> MAX_ENTRIES = AttributeDefinition.builder("maxEntries", -1).immutable().build();
   public static final AttributeDefinition<Float> FRAGMENTATION_FACTOR = AttributeDefinition.builder("fragmentationFactor", 0.75f).immutable().build();
   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SingleFileStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), LOCATION, MAX_ENTRIES, FRAGMENTATION_FACTOR);
   }

   private final Attribute<String> location;
   private final Attribute<Integer> maxEntries;
   private final Attribute<Float> fragmentationFactor;

   public SingleFileStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async,
                                       SingletonStoreConfiguration singletonStore) {
      super(attributes, async, singletonStore);
      location = attributes.attribute(LOCATION);
      maxEntries = attributes.attribute(MAX_ENTRIES);
      fragmentationFactor = attributes.attribute(FRAGMENTATION_FACTOR);
   }

   public String location() {
      return location.get();
   }

   public int maxEntries() {
      return maxEntries.get();
   }

   public float fragmentationFactor () {
      return fragmentationFactor.get();
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
