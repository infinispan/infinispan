package org.infinispan.configuration.global;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.security.PrincipalRoleMapper;
import org.infinispan.security.impl.ClusterRoleMapper;
import org.infinispan.security.impl.CommonNameRoleMapper;
import org.infinispan.security.impl.IdentityRoleMapper;

/**
 * @since 10.0
 */
public class PrincipalRoleMapperConfiguration implements ConfigurationInfo {

   public static final AttributeDefinition<Class> CLASS = AttributeDefinition.builder("class", null, Class.class).immutable().build();

   private final ElementDefinition elementDefinition;
   private final PrincipalRoleMapper principalRoleMapper;

   public static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(PrincipalRoleMapperConfiguration.class, CLASS);
   }

   private final AttributeSet attributes;

   PrincipalRoleMapperConfiguration(AttributeSet attributeSet, PrincipalRoleMapper principalRoleMapper) {
      this.attributes = attributeSet;
      this.principalRoleMapper = principalRoleMapper;
      this.elementDefinition = getElementDefinition(principalRoleMapper);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public PrincipalRoleMapper roleMapper() {
      return principalRoleMapper;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return elementDefinition;
   }

   static boolean isCustomMapper(PrincipalRoleMapper mapper) {
      return !(mapper instanceof IdentityRoleMapper) &&
            !(mapper instanceof CommonNameRoleMapper) &&
            !(mapper instanceof ClusterRoleMapper);
   }

   private ElementDefinition getElementDefinition(PrincipalRoleMapper mapper) {
      String elementName;
      if (isCustomMapper(mapper)) {
         elementName = Element.CUSTOM_ROLE_MAPPER.getLocalName();
      } else {
         if (mapper instanceof IdentityRoleMapper) {
            elementName = Element.IDENTITY_ROLE_MAPPER.getLocalName();
         } else if (mapper instanceof CommonNameRoleMapper) {
            elementName = Element.COMMON_NAME_ROLE_MAPPER.getLocalName();
         } else if (mapper instanceof ClusterRoleMapper) {
            elementName = Element.CLUSTER_ROLE_MAPPER.getLocalName();
         } else {
            elementName = Element.CUSTOM_ROLE_MAPPER.getLocalName();
         }
      }
      return new DefaultElementDefinition(elementName, true, false);
   }
}
