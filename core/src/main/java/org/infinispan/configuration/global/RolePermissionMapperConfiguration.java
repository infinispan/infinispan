package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.configuration.parsing.Attribute;
import org.infinispan.security.RolePermissionMapper;

/**
 * @since 14.0
 */
public class RolePermissionMapperConfiguration extends ConfigurationElement<RolePermissionMapperConfiguration> {

   public static final AttributeDefinition<Class> CLASS = AttributeDefinition.builder(Attribute.CLASS, null, Class.class).immutable().build();

   private final RolePermissionMapper permissionMapper;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RolePermissionMapperConfiguration.class, CLASS);
   }

   RolePermissionMapperConfiguration(AttributeSet attributeSet, RolePermissionMapper permissionMapper) {
      super("permission-mapper", attributeSet);
      this.permissionMapper = permissionMapper;
   }

   public RolePermissionMapper permissionMapper() {
      return permissionMapper;
   }
}
