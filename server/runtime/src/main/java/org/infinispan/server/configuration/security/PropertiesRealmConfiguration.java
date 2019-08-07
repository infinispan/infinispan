package org.infinispan.server.configuration.security;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class PropertiesRealmConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> GROUPS_ATTRIBUTE = AttributeDefinition.builder("groupsAttribute", null, String.class).build();

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.PROPERTIES_REALM.toString());

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(PropertiesRealmConfiguration.class, GROUPS_ATTRIBUTE);
   }

   private final AttributeSet attributes;
   private final UserPropertiesConfiguration userPropertiesConfiguration;
   private final GroupsPropertiesConfiguration groupsPropertiesConfiguration;
   private final List<ConfigurationInfo> subElements;

   PropertiesRealmConfiguration(AttributeSet attributes, UserPropertiesConfiguration userPropertiesConfiguration,
                                GroupsPropertiesConfiguration groupsPropertiesConfiguration) {
      this.attributes = attributes;
      this.userPropertiesConfiguration = userPropertiesConfiguration;
      this.groupsPropertiesConfiguration = groupsPropertiesConfiguration;
      this.subElements = Arrays.asList(userPropertiesConfiguration, groupsPropertiesConfiguration);
   }

   UserPropertiesConfiguration userProperties() {
      return userPropertiesConfiguration;
   }

   GroupsPropertiesConfiguration groupProperties() {
      return groupsPropertiesConfiguration;
   }

   public String groupAttribute() {
      return attributes.attribute(GROUPS_ATTRIBUTE).get();
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PropertiesRealmConfiguration that = (PropertiesRealmConfiguration) o;

      if (!attributes.equals(that.attributes)) return false;
      if (!userPropertiesConfiguration.equals(that.userPropertiesConfiguration)) return false;
      return groupsPropertiesConfiguration.equals(that.groupsPropertiesConfiguration);
   }

   @Override
   public int hashCode() {
      int result = attributes.hashCode();
      result = 31 * result + userPropertiesConfiguration.hashCode();
      result = 31 * result + groupsPropertiesConfiguration.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "PropertiesRealmConfiguration{" +
            "attributes=" + attributes +
            ", userPropertiesConfiguration=" + userPropertiesConfiguration +
            ", groupsPropertiesConfiguration=" + groupsPropertiesConfiguration +
            '}';
   }

}
