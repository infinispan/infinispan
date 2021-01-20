package org.infinispan.configuration.global;

import static org.infinispan.configuration.global.PrincipalRoleMapperConfiguration.CLASS;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.mappers.ClusterRoleMapper;

public class PrincipalRoleMapperConfigurationBuilder extends AbstractGlobalConfigurationBuilder implements Builder<PrincipalRoleMapperConfiguration> {
   private final AttributeSet attributes;
   private PrincipalRoleMapper principalRoleMapper = new ClusterRoleMapper();

   PrincipalRoleMapperConfigurationBuilder(GlobalConfigurationBuilder globalConfig) {
      super(globalConfig);
      attributes = PrincipalRoleMapperConfiguration.attributeDefinitionSet();
   }

   public PrincipalRoleMapperConfigurationBuilder mapper(PrincipalRoleMapper principalRoleMapper) {
      this.principalRoleMapper = principalRoleMapper;
      return this;
   }

   @Override
   public void validate() {
      if (principalRoleMapper != null && PrincipalRoleMapperConfiguration.isCustomMapper(principalRoleMapper)) {
         attributes.attribute(CLASS).set(principalRoleMapper.getClass());
      }
   }

   @Override
   public PrincipalRoleMapperConfiguration create() {
      return new PrincipalRoleMapperConfiguration(attributes.protect(), principalRoleMapper);
   }

   @Override
   public PrincipalRoleMapperConfigurationBuilder read(PrincipalRoleMapperConfiguration template) {
      attributes.read(template.attributes());
      this.principalRoleMapper = template.roleMapper();
      return this;
   }

   public PrincipalRoleMapper mapper() {
      return principalRoleMapper;
   }
}
