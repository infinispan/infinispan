package org.infinispan.persistence.remote.configuration.global;

import static org.infinispan.commons.configuration.AbstractTypedPropertiesConfiguration.PROPERTIES;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.Immutables;
import org.infinispan.commons.util.TypedProperties;
import org.infinispan.persistence.remote.configuration.Attribute;
import org.infinispan.persistence.remote.configuration.Element;

/**
 * @since 15.0
 **/
public class RemoteContainerConfiguration extends ConfigurationElement<RemoteContainerConfiguration> {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, "", String.class).immutable().build();
   static final AttributeDefinition<String> URI = AttributeDefinition.builder(Attribute.URI, null, String.class).immutable().build();
   private final org.infinispan.commons.configuration.attributes.Attribute<TypedProperties> properties;

   protected RemoteContainerConfiguration(AttributeSet attributes, ConfigurationElement<?>... children) {
      super(Element.REMOTE_CACHE_CONTAINER, attributes, children);
      this.properties = attributes.attribute(PROPERTIES);
      if (properties.isModified()) {
         properties.set(Immutables.immutableTypedProperties(properties.get()));
      }
   }

   public static AttributeSet attributeSet() {
      return new AttributeSet(RemoteContainerConfiguration.class, NAME, URI);
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String uri() {
      return attributes.attribute(URI).get();
   }

   public TypedProperties properties() {
      return properties.get();
   }
}
