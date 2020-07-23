package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.PropertiesRealmConfiguration.GROUPS_ATTRIBUTE;

import java.io.File;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.security.ServerSecurityRealm;
import org.infinispan.server.security.realm.PropertiesSecurityRealm;

public class PropertiesRealmConfigurationBuilder implements Builder<PropertiesRealmConfiguration> {

   private final UserPropertiesConfigurationBuilder userProperties = new UserPropertiesConfigurationBuilder();
   private final GroupsPropertiesConfigurationBuilder groupProperties = new GroupsPropertiesConfigurationBuilder();
   private final AttributeSet attributes;
   private final RealmConfigurationBuilder realmBuilder;
   private PropertiesSecurityRealm securityRealm;

   PropertiesRealmConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
      this.attributes = PropertiesRealmConfiguration.attributeDefinitionSet();
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

   public PropertiesSecurityRealm build() {
      if (securityRealm == null && attributes.isModified()) {
         File usersFile = userProperties.getFile();
         File groupsFile = groupProperties.getFile();
         String groupsAttribute = attributes.attribute(GROUPS_ATTRIBUTE).get();
         boolean plainText = userProperties.plainText();
         String realmName = userProperties.digestRealmName();
         PropertiesSecurityRealm propertiesSecurityRealm = new PropertiesSecurityRealm(usersFile, groupsFile, plainText, groupsAttribute, realmName);
         realmBuilder.addRealm(realmName, propertiesSecurityRealm);
         this.securityRealm = propertiesSecurityRealm;
         this.realmBuilder.setHttpChallengeReadiness(() -> !propertiesSecurityRealm.isEmpty());
         realmBuilder.addFeature(ServerSecurityRealm.Feature.PASSWORD);
      }
      return securityRealm;
   }

   @Override
   public PropertiesRealmConfiguration create() {
      return new PropertiesRealmConfiguration(attributes.protect(), userProperties.create(), groupProperties.create());
   }

   @Override
   public PropertiesRealmConfigurationBuilder read(PropertiesRealmConfiguration template) {
      attributes.read(template.attributes());
      userProperties.read(template.userProperties());
      groupProperties.read(template.groupProperties());
      return this;
   }
}
