package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.auth.realm.FileSystemSecurityRealm;

/**
 * @since 10.0
 */
public class FileSystemRealmConfiguration extends ConfigurationElement<FileSystemRealmConfiguration> {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).autoPersist(false).build();
   static final AttributeDefinition<Integer> LEVELS = AttributeDefinition.builder(Attribute.LEVELS, null, Integer.class).build();
   static final AttributeDefinition<Boolean> ENCODED = AttributeDefinition.builder(Attribute.ENCODED, null, Boolean.class).build();

   private final FileSystemSecurityRealm securityRealm;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(FileSystemRealmConfiguration.class, NAME, PATH, RELATIVE_TO, LEVELS, ENCODED);
   }

   FileSystemRealmConfiguration(AttributeSet attributes, FileSystemSecurityRealm securityRealm) {
      super(Element.FILESYSTEM_REALM, attributes);
      this.securityRealm = securityRealm;
   }

   public FileSystemSecurityRealm getSecurityRealm() {
      return securityRealm;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }
}
