package org.infinispan.server.configuration.security;

import java.io.File;
import java.util.Properties;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.security.ServerSecurityRealm;
import org.infinispan.server.security.realm.PropertiesSecurityRealm;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;

/**
 * @since 10.0
 */
@BuiltBy(PropertiesRealmConfigurationBuilder.class)
public class PropertiesRealmConfiguration extends ConfigurationElement<PropertiesRealmConfiguration> implements RealmProvider {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, "properties", String.class).immutable().build();
   static final AttributeDefinition<String> GROUPS_ATTRIBUTE = AttributeDefinition.builder(Attribute.GROUPS_ATTRIBUTE, "groups", String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(PropertiesRealmConfiguration.class, NAME, GROUPS_ATTRIBUTE);
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

   @Override
   public SecurityRealm build(SecurityConfiguration securityConfiguration, RealmConfiguration realm, SecurityDomain.Builder domainBuilder, Properties properties) {
      File usersFile = userPropertiesConfiguration.getFile(properties);
      File groupsFile = groupsPropertiesConfiguration.getFile(properties);
      String groupsAttribute = attributes.attribute(GROUPS_ATTRIBUTE).get();
      boolean plainText = userPropertiesConfiguration.plainText();
      String realmName = userPropertiesConfiguration.digestRealmName();
      PropertiesSecurityRealm propertiesSecurityRealm = new PropertiesSecurityRealm(usersFile, groupsFile, plainText, groupsAttribute, realmName);
      realm.setHttpChallengeReadiness(() -> !propertiesSecurityRealm.isEmpty());
      realm.addFeature(ServerSecurityRealm.Feature.PASSWORD);
      return propertiesSecurityRealm;
   }

   @Override
   public String name() {
      return attributes.attribute(NAME).get();
   }
}
