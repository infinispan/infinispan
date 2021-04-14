package org.infinispan.server.configuration.security;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class LdapIdentityMappingConfiguration extends ConfigurationElement<LdapIdentityMappingConfiguration> {

   static final AttributeDefinition<String> RDN_IDENTIFIER = AttributeDefinition.builder(Attribute.RDN_IDENTIFIER, null, String.class).immutable().build();
   static final AttributeDefinition<String> SEARCH_BASE_DN = AttributeDefinition.builder(Attribute.SEARCH_DN, null, String.class).immutable().build();
   static final AttributeDefinition<Boolean> SEARCH_RECURSIVE = AttributeDefinition.builder(Attribute.SEARCH_RECURSIVE, false, Boolean.class).immutable().build();
   static final AttributeDefinition<String> FILTER_NAME = AttributeDefinition.builder(Attribute.FILTER_NAME, "(rdn_identifier={0})", String.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapIdentityMappingConfiguration.class, RDN_IDENTIFIER, SEARCH_BASE_DN, SEARCH_RECURSIVE, FILTER_NAME);
   }

   private final List<LdapAttributeMappingConfiguration> attributeMappings;
   private final List<LdapUserPasswordMapperConfiguration> userPasswordMapper;

   LdapIdentityMappingConfiguration(AttributeSet attributes,
                                    List<LdapAttributeMappingConfiguration> attributeMappings,
                                    List<LdapUserPasswordMapperConfiguration> userPasswordMapper) {
      super(Element.IDENTITY_MAPPING, attributes);
      this.attributeMappings = attributeMappings;
      this.userPasswordMapper = userPasswordMapper;
   }

   public List<LdapAttributeMappingConfiguration> attributeMappings() {
      return attributeMappings;
   }

   public List<LdapUserPasswordMapperConfiguration> userPasswordMapper() {
      return userPasswordMapper;
   }
}
