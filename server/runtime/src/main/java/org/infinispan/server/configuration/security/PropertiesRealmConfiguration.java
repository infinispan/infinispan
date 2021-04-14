package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class PropertiesRealmConfiguration extends ConfigurationElement<PropertiesRealmConfiguration> {
   static final AttributeDefinition<String> GROUPS_ATTRIBUTE = AttributeDefinition.builder(Attribute.GROUPS_ATTRIBUTE, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(PropertiesRealmConfiguration.class, GROUPS_ATTRIBUTE);
   }

   private final UserPropertiesConfiguration userPropertiesConfiguration;
   private final GroupsPropertiesConfiguration groupsPropertiesConfiguration;

   PropertiesRealmConfiguration(AttributeSet attributes, UserPropertiesConfiguration userPropertiesConfiguration,
                                GroupsPropertiesConfiguration groupsPropertiesConfiguration) {
      super(Element.PROPERTIES_REALM, attributes, userPropertiesConfiguration, groupsPropertiesConfiguration);
      this.userPropertiesConfiguration = userPropertiesConfiguration;
      this.groupsPropertiesConfiguration = groupsPropertiesConfiguration;
   }

   public UserPropertiesConfiguration userProperties() {
      return userPropertiesConfiguration;
   }

   public GroupsPropertiesConfiguration groupProperties() {
      return groupsPropertiesConfiguration;
   }

   public String groupAttribute() {
      return attributes.attribute(GROUPS_ATTRIBUTE).get();
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
