package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.GroupsPropertiesConfiguration.PATH;
import static org.infinispan.server.configuration.security.GroupsPropertiesConfiguration.RELATIVE_TO;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class GroupsPropertiesConfigurationBuilder implements Builder<GroupsPropertiesConfiguration> {
   private final AttributeSet attributes;

   GroupsPropertiesConfigurationBuilder() {
      attributes = GroupsPropertiesConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public GroupsPropertiesConfigurationBuilder path(String path) {
      attributes.attribute(PATH).set(path);
      return this;
   }

   public GroupsPropertiesConfigurationBuilder relativeTo(String relativeTo) {
      attributes.attribute(RELATIVE_TO).set(relativeTo);
      return this;
   }

   @Override
   public GroupsPropertiesConfiguration create() {
      return new GroupsPropertiesConfiguration(attributes.protect());
   }

   @Override
   public GroupsPropertiesConfigurationBuilder read(GroupsPropertiesConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      return this;
   }
}
