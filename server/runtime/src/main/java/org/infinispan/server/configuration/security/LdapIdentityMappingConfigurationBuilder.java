package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.LdapAttributeConfiguration.SEARCH_RECURSIVE;
import static org.infinispan.server.configuration.security.LdapIdentityMappingConfiguration.FILTER_NAME;
import static org.infinispan.server.configuration.security.LdapIdentityMappingConfiguration.RDN_IDENTIFIER;
import static org.infinispan.server.configuration.security.LdapIdentityMappingConfiguration.SEARCH_BASE_DN;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class LdapIdentityMappingConfigurationBuilder implements Builder<LdapIdentityMappingConfiguration> {
   private final AttributeSet attributes;
   private final List<LdapAttributeMappingConfigurationBuilder> attributeMappings = new ArrayList<>();
   private final List<LdapUserPasswordMapperConfigurationBuilder> userPasswordMapper = new ArrayList<>();

   private final LdapRealmConfigurationBuilder ldapConfigurationBuilder;

   LdapIdentityMappingConfigurationBuilder(LdapRealmConfigurationBuilder ldapConfigurationBuilder) {
      this.ldapConfigurationBuilder = ldapConfigurationBuilder;
      this.attributes = LdapIdentityMappingConfiguration.attributeDefinitionSet();
   }

   public LdapIdentityMappingConfigurationBuilder rdnIdentifier(String rdnIdentifier) {
      attributes.attribute(RDN_IDENTIFIER).set(rdnIdentifier);
      ldapConfigurationBuilder.getIdentityMappingBuilder().setRdnIdentifier(rdnIdentifier);
      return this;
   }

   public LdapIdentityMappingConfigurationBuilder searchBaseDn(String searchBaseDn) {
      attributes.attribute(SEARCH_BASE_DN).set(searchBaseDn);
      ldapConfigurationBuilder.getIdentityMappingBuilder().setSearchDn(searchBaseDn);
      return this;
   }

   public LdapIdentityMappingConfigurationBuilder searchRecursive(boolean searchRecursive) {
      attributes.attribute(SEARCH_RECURSIVE).set(searchRecursive);
      if (searchRecursive) {
         ldapConfigurationBuilder.getIdentityMappingBuilder().searchRecursive();
      }
      return this;
   }

   public LdapIdentityMappingConfigurationBuilder filterName(String filterName) {
      attributes.attribute(FILTER_NAME).set(filterName);
      ldapConfigurationBuilder.getIdentityMappingBuilder().setFilterName(filterName);
      return this;
   }

   public LdapAttributeMappingConfigurationBuilder addAttributeMapping() {
      LdapAttributeMappingConfigurationBuilder builder = new LdapAttributeMappingConfigurationBuilder(ldapConfigurationBuilder);
      attributeMappings.add(builder);
      return builder;
   }

   public LdapUserPasswordMapperConfigurationBuilder addUserPasswordMapper() {
      LdapUserPasswordMapperConfigurationBuilder builder = new LdapUserPasswordMapperConfigurationBuilder(ldapConfigurationBuilder);
      userPasswordMapper.add(builder);
      return builder;
   }

   @Override
   public void validate() {
      attributeMappings.forEach(LdapAttributeMappingConfigurationBuilder::validate);
      userPasswordMapper.forEach(LdapUserPasswordMapperConfigurationBuilder::validate);
   }

   @Override
   public LdapIdentityMappingConfiguration create() {
      List<LdapAttributeMappingConfiguration> mappingConfigurations = attributeMappings.stream()
            .map(LdapAttributeMappingConfigurationBuilder::create).collect(Collectors.toList());
      List<LdapUserPasswordMapperConfiguration> userPasswordMapperConfigurations = userPasswordMapper.stream()
            .map(LdapUserPasswordMapperConfigurationBuilder::create).collect(Collectors.toList());

      return new LdapIdentityMappingConfiguration(attributes.protect(), mappingConfigurations, userPasswordMapperConfigurations);
   }

   @Override
   public LdapIdentityMappingConfigurationBuilder read(LdapIdentityMappingConfiguration template) {
      attributes.read(template.attributes());
      attributeMappings.clear();
      userPasswordMapper.clear();
      template.attributeMappings().forEach(a -> addAttributeMapping().read(a));
      template.userPasswordMapper().forEach(a -> addUserPasswordMapper().read(a));
      return this;
   }
}
