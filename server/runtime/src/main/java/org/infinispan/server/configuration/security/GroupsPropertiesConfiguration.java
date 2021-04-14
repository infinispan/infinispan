package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

public class GroupsPropertiesConfiguration extends ConfigurationElement<GroupsPropertiesConfiguration> {
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, null, String.class).autoPersist(false).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(GroupsPropertiesConfiguration.class, PATH, RELATIVE_TO);
   }

   GroupsPropertiesConfiguration(AttributeSet attributes) {
      super(Element.GROUP_PROPERTIES, attributes);
   }

   public String path() {
      return attributes.attribute(PATH).get();
   }

   public String relativeTo() {
      return attributes.attribute(RELATIVE_TO).get();
   }
}
