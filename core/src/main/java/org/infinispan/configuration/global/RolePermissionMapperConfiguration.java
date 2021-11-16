package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.security.RolePermissionMapper;

/**
 * @since 14.0
 */
public class RolePermissionMapperConfiguration {

   public static final AttributeDefinition<Class> CLASS = AttributeDefinition.builder("class", null, Class.class).immutable().build();

   private final RolePermissionMapper permissionMapper;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RolePermissionMapperConfiguration.class, CLASS);
   }

   private final AttributeSet attributes;

   RolePermissionMapperConfiguration(AttributeSet attributeSet, RolePermissionMapper permissionMapper) {
      this.attributes = attributeSet;
      this.permissionMapper = permissionMapper;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public RolePermissionMapper permissionMapper() {
      return permissionMapper;
   }
}
