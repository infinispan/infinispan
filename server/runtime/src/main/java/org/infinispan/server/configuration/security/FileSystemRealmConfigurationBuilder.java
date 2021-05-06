package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.FileSystemRealmConfiguration.ENCODED;
import static org.infinispan.server.configuration.security.FileSystemRealmConfiguration.LEVELS;
import static org.infinispan.server.configuration.security.FileSystemRealmConfiguration.NAME;
import static org.infinispan.server.configuration.security.FileSystemRealmConfiguration.PATH;
import static org.infinispan.server.configuration.security.FileSystemRealmConfiguration.RELATIVE_TO;

import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class FileSystemRealmConfigurationBuilder implements RealmProviderBuilder<FileSystemRealmConfiguration> {
   private final AttributeSet attributes;

   FileSystemRealmConfigurationBuilder() {
      this.attributes = FileSystemRealmConfiguration.attributeDefinitionSet();
   }

   public FileSystemRealmConfigurationBuilder name(String name) {
      attributes.attribute(NAME).set(name);
      return this;
   }

   @Override
   public String name() {
      return attributes.attribute(DistributedRealmConfiguration.NAME).get();
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
   public FileSystemRealmConfiguration create() {
      return new FileSystemRealmConfiguration(attributes.protect());
   }

   @Override
   public FileSystemRealmConfigurationBuilder read(FileSystemRealmConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

   @Override
   public int compareTo(RealmProviderBuilder o) {
      return 0; // Irrelevant
   }
}
