package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class LdapIdentityMappingConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<String> RDN_IDENTIFIER = AttributeDefinition.builder("rdnIdentifier", null, String.class).build();
   static final AttributeDefinition<String> SEARCH_BASE_DN = AttributeDefinition.builder("searchBaseDn", null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapIdentityMappingConfiguration.class, RDN_IDENTIFIER, SEARCH_BASE_DN);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.IDENTITY_MAPPING.toString());

   private final AttributeSet attributes;
   private final List<LdapAttributeMappingConfiguration> attributeMappings;
   private final List<LdapUserPasswordMapperConfiguration> userPasswordMapper;

   private final List<ConfigurationInfo> subElements = new ArrayList<>();

   LdapIdentityMappingConfiguration(AttributeSet attributes,
                                    List<LdapAttributeMappingConfiguration> attributeMappings,
                                    List<LdapUserPasswordMapperConfiguration> userPasswordMapper) {
      this.attributes = attributes.checkProtection();
      this.attributeMappings = attributeMappings;
      this.userPasswordMapper = userPasswordMapper;
      this.subElements.addAll(attributeMappings);
      this.subElements.addAll(userPasswordMapper);
   }

   List<LdapAttributeMappingConfiguration> attributeMappings() {
      return attributeMappings;
   }

   List<LdapUserPasswordMapperConfiguration> userPasswordMapper() {
      return userPasswordMapper;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return subElements;
   }
}
