package org.infinispan.persistence.file;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfiguration;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.parsing.Element;

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
   public static final AttributeDefinition<Float> FRAGMENTATION_FACTOR = AttributeDefinition.builder(org.infinispan.configuration.parsing.Attribute.FRAGMENTATION_FACTOR, 0.75f).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SingleFileStoreConfiguration.class, AbstractStoreConfiguration.attributeDefinitionSet(), LOCATION, FRAGMENTATION_FACTOR);
   }

   public SingleFileStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async) {
      super(Element.SINGLE_FILE_STORE, attributes, async);
   }

   public String location() {
      return attributes.attribute(LOCATION).get();
   }

   public float fragmentationFactor() {
      return attributes.attribute(FRAGMENTATION_FACTOR).get();
   }
}
