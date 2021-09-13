package org.infinispan.server.configuration.security;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.auth.realm.ldap.LdapSecurityRealmBuilder;

/**
 * @since 10.0
 */
public class LdapIdentityMappingConfiguration extends ConfigurationElement<LdapIdentityMappingConfiguration> {

   static final AttributeDefinition<String> RDN_IDENTIFIER = AttributeDefinition.builder(Attribute.RDN_IDENTIFIER, null, String.class).immutable().build();
   static final AttributeDefinition<String> SEARCH_BASE_DN = AttributeDefinition.builder(Attribute.SEARCH_DN, null, String.class).immutable().build();
   static final AttributeDefinition<Boolean> SEARCH_RECURSIVE = AttributeDefinition.builder(Attribute.SEARCH_RECURSIVE, false, Boolean.class).immutable().build();
   static final AttributeDefinition<Integer> SEARCH_TIME_LIMIT = AttributeDefinition.builder(Attribute.SEARCH_TIME_LIMIT, 10_000, Integer.class).immutable().build();
   static final AttributeDefinition<String> FILTER_NAME = AttributeDefinition.builder(Attribute.FILTER_NAME, "(rdn_identifier={0})", String.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapIdentityMappingConfiguration.class, RDN_IDENTIFIER, SEARCH_BASE_DN, SEARCH_RECURSIVE, SEARCH_TIME_LIMIT, FILTER_NAME);
   }

   private final List<LdapAttributeConfiguration> attributeMappings;
   private final LdapUserPasswordMapperConfiguration userPasswordMapper;

   LdapIdentityMappingConfiguration(AttributeSet attributes,
                                    List<LdapAttributeConfiguration> attributeMappings,
                                    LdapUserPasswordMapperConfiguration userPasswordMapper) {
      super(Element.IDENTITY_MAPPING, attributes);
      this.attributeMappings = attributeMappings;
      this.userPasswordMapper = userPasswordMapper;
   }

   public List<LdapAttributeConfiguration> attributeMappings() {
      return attributeMappings;
   }

   public LdapUserPasswordMapperConfiguration userPasswordMapper() {
      return userPasswordMapper;
   }

   void build(LdapSecurityRealmBuilder builder) {
      LdapSecurityRealmBuilder.IdentityMappingBuilder identity = builder.identityMapping();
      if (attributes.attribute(RDN_IDENTIFIER).isModified()) {
         identity.setRdnIdentifier(attributes.attribute(RDN_IDENTIFIER).get());
      }
      if (attributes.attribute(SEARCH_BASE_DN).isModified()) {
         identity.setSearchDn(attributes.attribute(SEARCH_BASE_DN).get());
      }
      if (attributes.attribute(SEARCH_RECURSIVE).get()) {
         identity.searchRecursive();
      }
      identity.setSearchTimeLimit(attributes.attribute(SEARCH_TIME_LIMIT).get());
      if (attributes.attribute(FILTER_NAME).isModified()) {
         identity.setFilterName(attributes.attribute(FILTER_NAME).get());
      }
      for (LdapAttributeConfiguration mapping : attributeMappings) {
         mapping.build(identity);
      }
      userPasswordMapper.build(builder);
      identity.build(); // side-effect
   }
}
