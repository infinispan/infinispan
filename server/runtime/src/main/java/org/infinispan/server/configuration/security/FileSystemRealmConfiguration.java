package org.infinispan.server.configuration.security;

import java.io.File;
import java.nio.file.Path;
import java.util.Properties;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.ParseUtils;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.auth.realm.FileSystemSecurityRealm;
import org.wildfly.security.auth.server.NameRewriter;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;

/**
 * @since 10.0
 */
@BuiltBy(FileSystemRealmConfigurationBuilder.class)
public class FileSystemRealmConfiguration extends ConfigurationElement<FileSystemRealmConfiguration> implements RealmProvider {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, "filesystem", String.class).build();
   static final AttributeDefinition<String> PATH = AttributeDefinition.builder(Attribute.PATH, null, String.class).build();
   static final AttributeDefinition<String> RELATIVE_TO = AttributeDefinition.builder(Attribute.RELATIVE_TO, Server.INFINISPAN_SERVER_CONFIG_PATH, String.class).autoPersist(false).build();
   static final AttributeDefinition<Integer> LEVELS = AttributeDefinition.builder(Attribute.LEVELS, null, Integer.class).build();
   static final AttributeDefinition<Boolean> ENCODED = AttributeDefinition.builder(Attribute.ENCODED, null, Boolean.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(FileSystemRealmConfiguration.class, NAME, PATH, RELATIVE_TO, LEVELS, ENCODED);
   }

   FileSystemRealmConfiguration(AttributeSet attributes) {
      super(Element.FILESYSTEM_REALM, attributes);
   }

   @Override
   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public SecurityRealm build(SecurityConfiguration securityConfiguration, RealmConfiguration realm, SecurityDomain.Builder domainBuilder, Properties properties) {
      String path = attributes.attribute(PATH).get();
      String relativeTo = properties.getProperty(attributes.attribute(RELATIVE_TO).get());
      Integer levels = attributes.attribute(LEVELS).get();
      Boolean encoded = attributes.attribute(ENCODED).get();
      Path filesystemPath = new File(ParseUtils.resolvePath(path, relativeTo)).toPath();
      return new FileSystemSecurityRealm(filesystemPath, NameRewriter.IDENTITY_REWRITER, levels, encoded);
   }
}
