package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.Element;
import org.jgroups.conf.ProtocolConfiguration;

/*
 * @since 10.0
 */
public class JGroupsProtocolConfiguration {

   static final AttributeDefinition<ProtocolConfiguration> PROTOCOL_CONFIG = AttributeDefinition.builder(Element.PROPERTIES, null, ProtocolConfiguration.class).immutable().build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JGroupsProtocolConfiguration.class, PROTOCOL_CONFIG);
   }

   private final Attribute<ProtocolConfiguration> protocolConfiguration;
   private final AttributeSet attributes;

   JGroupsProtocolConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.protocolConfiguration = attributes.attribute(PROTOCOL_CONFIG);
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public ProtocolConfiguration protocolConfiguration() {
      return protocolConfiguration.get();
   }

   @Override
   public String toString() {
      return "JGroupsProtocolConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
