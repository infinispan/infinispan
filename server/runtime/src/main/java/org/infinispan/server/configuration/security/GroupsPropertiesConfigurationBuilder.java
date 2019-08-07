package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.GroupsPropertiesConfiguration.PATH;
import static org.infinispan.server.configuration.security.GroupsPropertiesConfiguration.RELATIVE_TO;

import java.io.File;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.parsing.ParseUtils;

/**
 * @since 10.0
 */
public class GroupsPropertiesConfigurationBuilder implements Builder<GroupsPropertiesConfiguration> {
   private final AttributeSet attributes;
   private File groupsFile;

   GroupsPropertiesConfigurationBuilder() {
      attributes = GroupsPropertiesConfiguration.attributeDefinitionSet();
   }

   public GroupsPropertiesConfigurationBuilder path(String path) {
      attributes.attribute(PATH).set(path);
      return this;
   }

   public GroupsPropertiesConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(RELATIVE_TO).set(relativeTo);
      return this;
   }

   public File getFile() {
      if (groupsFile == null) {
         String path = attributes.attribute(PATH).get();
         String relativeTo = attributes.attribute(RELATIVE_TO).get();
         groupsFile = new File(ParseUtils.resolvePath(path, relativeTo));
      }
      return groupsFile;
   }

   @Override
   public void validate() {
   }

   @Override
   public GroupsPropertiesConfiguration create() {
      return new GroupsPropertiesConfiguration(attributes.protect());
   }

   @Override
   public GroupsPropertiesConfigurationBuilder read(GroupsPropertiesConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }
}
