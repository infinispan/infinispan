package org.infinispan.configuration.cache;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.persistence.file.SingleFileStore;

/**
 * Defines the configuration for the single file cache store.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@BuiltBy(SingleFileStoreConfigurationBuilder.class)
@ConfigurationFor(SingleFileStore.class)
public class SingleFileStoreConfiguration extends AbstractStoreConfiguration<SingleFileStoreConfiguration> {
   public static final AttributeDefinition<String> LOCATION = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.PATH, null, String.class).immutable().global(false).build();
   /**
    * @deprecated Since 13.0, will be removed in 16.0
    */
   @Deprecated
   public static final AttributeDefinition<Integer> MAX_ENTRIES = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.MAX_ENTRIES, -1).immutable().build();
   public static final AttributeDefinition<Float> FRAGMENTATION_FACTOR = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.FRAGMENTATION_FACTOR, 0.75f).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SingleFileStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), LOCATION, MAX_ENTRIES, FRAGMENTATION_FACTOR);
   }

   public SingleFileStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async) {
      super(Element.SINGLE_FILE_STORE, attributes, async);
   }

   public String location() {
      return attributes.attribute(LOCATION).get();
   }

   /**
    * @deprecated Since 13.0, will be removed in 16.0.
    */
   @Deprecated
   public int maxEntries() {
      return attributes.attribute(MAX_ENTRIES).get();
   }

   public float fragmentationFactor() {
      return attributes.attribute(FRAGMENTATION_FACTOR).get();
   }
}
