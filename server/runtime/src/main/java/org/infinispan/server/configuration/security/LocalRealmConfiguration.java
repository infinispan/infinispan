package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class LocalRealmConfiguration extends ConfigurationElement<LocalRealmConfiguration> {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LocalRealmConfiguration.class, NAME);
   }

   LocalRealmConfiguration(AttributeSet attributes) {
      super(Element.LOCAL_REALM, attributes);
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }
}
