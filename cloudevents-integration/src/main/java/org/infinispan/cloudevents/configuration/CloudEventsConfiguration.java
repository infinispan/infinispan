package org.infinispan.cloudevents.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.commons.util.Experimental;
import org.infinispan.configuration.serializing.SerializedWith;

/**
 * Configuration module to control the CloudEvents integration for a cache.
 *
 * @since 12
 * @author Dan Berindei
 */
@Experimental
@SerializedWith(CloudEventsConfigurationSerializer.class)
@BuiltBy(CloudEventsConfigurationBuilder.class)
public class CloudEventsConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<Boolean> ENABLED =
         AttributeDefinition.builder("enabled", true).immutable().build();

   private final ElementDefinition<ConfigurationInfo> elementDefinition;
   private final AttributeSet attributes;

   public CloudEventsConfiguration(AttributeSet attributes) {
      this.elementDefinition = new DefaultElementDefinition<>(Element.CLOUDEVENTS.getLocalName(), true, false);
      this.attributes = attributes;
   }

   public static AttributeSet attributeSet() {
      return new AttributeSet(CloudEventsConfiguration.class, ENABLED);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public boolean enabled() {
      return attributes.attribute(ENABLED).get();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return elementDefinition;
   }

   @Override
   public String toString() {
      return "CloudEventsConfiguration" + attributes.toString(null);
   }
}
