package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.PropertiesRealmConfiguration.GROUPS_ATTRIBUTE;
import static org.infinispan.server.configuration.security.PropertiesRealmConfiguration.NAME;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class PropertiesRealmConfigurationBuilder implements RealmProviderBuilder<PropertiesRealmConfiguration> {

   private final UserPropertiesConfigurationBuilder userProperties = new UserPropertiesConfigurationBuilder();
   private final GroupsPropertiesConfigurationBuilder groupProperties = new GroupsPropertiesConfigurationBuilder();
   private final AttributeSet attributes;

   PropertiesRealmConfigurationBuilder() {
      this.attributes = PropertiesRealmConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public PropertiesRealmConfigurationBuilder name(String name) {
      attributes.attribute(NAME).set(name);
      return this;
   }

   @Override
   public String name() {
      return attributes.attribute(DistributedRealmConfiguration.NAME).get();
   }

   public PropertiesRealmConfigurationBuilder groupAttribute(String groupAttribute) {
      attributes.attribute(GROUPS_ATTRIBUTE).set(groupAttribute);
      return this;
   }

   public UserPropertiesConfigurationBuilder userProperties() {
      return userProperties;
   }

   public GroupsPropertiesConfigurationBuilder groupProperties() {
      return groupProperties;
   }

   @Override
   public void validate() {
      userProperties.validate();
      groupProperties.validate();
   }

   @Override
   public PropertiesRealmConfiguration create() {
      return new PropertiesRealmConfiguration(attributes.protect(), userProperties.create(), groupProperties.create());
   }

   @Override
   public PropertiesRealmConfigurationBuilder read(PropertiesRealmConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      userProperties.read(template.userProperties(), combine);
      groupProperties.read(template.groupProperties(), combine);
      return this;
   }

   @Override
   public int compareTo(RealmProviderBuilder o) {
      return 0; // Irrelevant
   }
}
