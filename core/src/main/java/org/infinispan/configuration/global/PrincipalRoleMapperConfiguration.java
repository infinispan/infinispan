package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.mappers.ClusterRoleMapper;
import org.infinispan.security.mappers.CommonNameRoleMapper;
import org.infinispan.security.mappers.IdentityRoleMapper;

/**
 * @since 10.0
 */
public class PrincipalRoleMapperConfiguration {

   public static final AttributeDefinition<Class> CLASS = AttributeDefinition.builder("class", null, Class.class).immutable().build();

   private final PrincipalRoleMapper principalRoleMapper;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(PrincipalRoleMapperConfiguration.class, CLASS);
   }

   private final AttributeSet attributes;

   PrincipalRoleMapperConfiguration(AttributeSet attributeSet, PrincipalRoleMapper principalRoleMapper) {
      this.attributes = attributeSet;
      this.principalRoleMapper = principalRoleMapper;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public PrincipalRoleMapper roleMapper() {
      return principalRoleMapper;
   }

   static boolean isCustomMapper(PrincipalRoleMapper mapper) {
      return !(mapper instanceof IdentityRoleMapper) &&
            !(mapper instanceof CommonNameRoleMapper) &&
            !(mapper instanceof ClusterRoleMapper);
   }
}
