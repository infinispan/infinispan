package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.FileSystemRealmConfiguration.ENCODED;
import static org.infinispan.server.configuration.security.FileSystemRealmConfiguration.LEVELS;
import static org.infinispan.server.configuration.security.FileSystemRealmConfiguration.NAME;
import static org.infinispan.server.configuration.security.FileSystemRealmConfiguration.PATH;
import static org.infinispan.server.configuration.security.FileSystemRealmConfiguration.RELATIVE_TO;

import java.io.File;
import java.nio.file.Path;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.ParseUtils;
import org.wildfly.security.auth.realm.FileSystemSecurityRealm;
import org.wildfly.security.auth.server.NameRewriter;

/**
 * @since 10.0
 */
public class FileSystemRealmConfigurationBuilder implements Builder<FileSystemRealmConfiguration> {
   private final AttributeSet attributes;
   private final RealmConfigurationBuilder realmBuilder;
   private FileSystemSecurityRealm securityRealm;

   FileSystemRealmConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.attributes = FileSystemRealmConfiguration.attributeDefinitionSet();
      this.realmBuilder = realmBuilder;
   }

   public FileSystemRealmConfigurationBuilder name(String name) {
      attributes.attribute(NAME).set(name);
      return this;
   }

   public FileSystemRealmConfigurationBuilder path(String path) {
      attributes.attribute(PATH).set(path);
      return this;
   }

   public FileSystemRealmConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(RELATIVE_TO).set(relativeTo);
      return this;
   }

   public FileSystemRealmConfigurationBuilder levels(int levels) {
      attributes.attribute(LEVELS).set(levels);
      return this;
   }

   public FileSystemRealmConfigurationBuilder encoded(boolean encoded) {
      attributes.attribute(ENCODED).set(encoded);
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public FileSystemRealmConfiguration create() {
      return new FileSystemRealmConfiguration(attributes.protect(), securityRealm);
   }

   @Override
   public FileSystemRealmConfigurationBuilder read(FileSystemRealmConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   public FileSystemSecurityRealm build() {
      if (securityRealm == null && attributes.isModified()) {
         String name = attributes.attribute(NAME).get();
         String path = attributes.attribute(PATH).get();
         String relativeTo = attributes.attribute(RELATIVE_TO).get();
         Integer levels = attributes.attribute(LEVELS).get();
         Boolean encoded = attributes.attribute(ENCODED).get();
         Path filesystemPath = new File(ParseUtils.resolvePath(path, relativeTo)).toPath();
         this.securityRealm = new FileSystemSecurityRealm(filesystemPath, NameRewriter.IDENTITY_REWRITER, levels, encoded);
         realmBuilder.addRealm(name, securityRealm);
      }
      return securityRealm;
   }
}
