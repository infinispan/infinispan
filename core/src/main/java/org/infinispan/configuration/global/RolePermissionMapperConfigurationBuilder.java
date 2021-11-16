package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.security.RolePermissionMapper;
import org.infinispan.security.mappers.ClusterPermissionMapper;

/**
 * @since 14.0
 */
public class RolePermissionMapperConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<RolePermissionMapperConfiguration> {
   private final AttributeSet attributes;
   private RolePermissionMapper permissionMapper = new ClusterPermissionMapper();

   RolePermissionMapperConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = RolePermissionMapperConfiguration.attributeDefinitionSet();
   }

   public RolePermissionMapperConfigurationBuilder mapper(RolePermissionMapper permissionMapper) {
      this.permissionMapper = permissionMapper;
      return this;
   }

   @Override
   public void validate() {
   }

   @Override
   public RolePermissionMapperConfiguration create() {
      return new RolePermissionMapperConfiguration(attributes.protect(), permissionMapper);
   }

   @Override
   public RolePermissionMapperConfigurationBuilder read(RolePermissionMapperConfiguration template) {
      attributes.read(template.attributes());
      this.permissionMapper = template.permissionMapper();
      return this;
   }

   public RolePermissionMapper mapper() {
      return permissionMapper;
   }
}
