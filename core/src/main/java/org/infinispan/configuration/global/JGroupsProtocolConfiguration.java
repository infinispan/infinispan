package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AsElementAttributeSerializer;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.jgroups.conf.ProtocolConfiguration;

/*
 * @since 10.0
 */
public class JGroupsProtocolConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<ProtocolConfiguration> PROTOCOL_CONFIG = AttributeDefinition
         .builder("protocolConfig", null, ProtocolConfiguration.class)
         .immutable().serializer(new AsElementAttributeSerializer<ProtocolConfiguration, JGroupsProtocolConfiguration, JGroupsProtocolConfigurationBuilder>() {
            @Override
            public String getSerializationName(Attribute<ProtocolConfiguration> attribute, JGroupsProtocolConfiguration configurationElement) {
               return attribute.get().getProtocolName();
            }

            @Override
            public Object getSerializationValue(Attribute<ProtocolConfiguration> attribute, JGroupsProtocolConfiguration configurationElement) {
               return attribute.get().getProperties();
            }
         }).build();

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JGroupsProtocolConfiguration.class, PROTOCOL_CONFIG);
   }

   public static ElementDefinition ELEMENT_DEFINITION = new ElementDefinition<JGroupsProtocolConfiguration>() {
      @Override
      public boolean isTopLevel() {
         return false;
      }

      @Override
      public ElementOutput toExternalName(JGroupsProtocolConfiguration configuration) {
         return new ElementOutput(configuration.protocolConfiguration().getProtocolName());
      }

      @Override
      public boolean supports(String elementName) {
         return true;
      }
   };

   private final Attribute<ProtocolConfiguration> protocolConfiguration;
   private final AttributeSet attributes;

   JGroupsProtocolConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.protocolConfiguration = attributes.attribute(PROTOCOL_CONFIG);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
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
