package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.auth.realm.FileSystemSecurityRealm;

/**
 * @since 10.0
 */
public class FileSystemRealmConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder("path", null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder("relativeTo", null, String.class).build();
   static final AttributeDefinition<Integer> LEVELS = AttributeDefinition.builder("levels", null, Integer.class).build();
   static final AttributeDefinition<Boolean> ENCODED = AttributeDefinition.builder("encoded", null, Boolean.class).build();

   private final AttributeSet attributes;
   private final FileSystemSecurityRealm securityRealm;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(FileSystemRealmConfiguration.class, NAME, PATH, RELATIVE_TO, LEVELS, ENCODED);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.FILESYSTEM_REALM.toString());

   FileSystemRealmConfiguration(AttributeSet attributes, FileSystemSecurityRealm securityRealm) {
      this.attributes = attributes.checkProtection();
      this.securityRealm = securityRealm;
   }

   public FileSystemSecurityRealm getSecurityRealm() {
      return securityRealm;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }
}
