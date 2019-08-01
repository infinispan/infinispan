package org.infinispan.persistence.rest.configuration;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
public class RemoteServerConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<String> HOST = AttributeDefinition.builder("host", null, String.class).immutable().autoPersist(false).build();
   static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder("port", 80).immutable().autoPersist(false).build();
   private final AttributeSet attributes;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RemoteServerConfiguration.class, HOST, PORT);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SERVER.getLocalName());

   private final Attribute<String> host;
   private final Attribute<Integer> port;

   RemoteServerConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      host = attributes.attribute(HOST);
      port = attributes.attribute(PORT);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public String host() {
      return host.get();
   }

   public int port() {
      return port.get();
   }

   @Override
   public String toString() {
      return "RemoteServerConfiguration{" +
            "attributes=" + attributes +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RemoteServerConfiguration that = (RemoteServerConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}
