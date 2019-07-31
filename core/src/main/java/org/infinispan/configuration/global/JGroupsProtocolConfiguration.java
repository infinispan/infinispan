package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.jgroups.conf.ProtocolConfiguration;

/*
 * @since 10.0
 */
public class JGroupsProtocolConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<ProtocolConfiguration> PROTOCOL_CONFIG = AttributeDefinition.builder("protocolConfig", null, ProtocolConfiguration.class).immutable().serializer(new AttributeSerializer<ProtocolConfiguration, JGroupsProtocolConfiguration, ConfigurationBuilderInfo>() {
      @Override
      public Object getSerializationValue(Attribute<ProtocolConfiguration> attribute, JGroupsProtocolConfiguration cfg) {
         return attribute.get().getProperties();
      }

      @Override
      public String getSerializationName(Attribute<ProtocolConfiguration> attribute, JGroupsProtocolConfiguration configurationElement) {
         return null;
      }
   }).xmlName("properties").build();

   private final ElementDefinition elementDefinition;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(JGroupsProtocolConfiguration.class, PROTOCOL_CONFIG);
   }

   private final Attribute<ProtocolConfiguration> protocolConfiguration;
   private final AttributeSet attributes;

   JGroupsProtocolConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.protocolConfiguration = attributes.attribute(PROTOCOL_CONFIG);
      this.elementDefinition = new DefaultElementDefinition(protocolConfiguration.get().getProtocolName(), true, false);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return elementDefinition;
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
