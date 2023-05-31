package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.LdapAttributeConfiguration.SEARCH_RECURSIVE;
import static org.infinispan.server.configuration.security.LdapIdentityMappingConfiguration.FILTER_NAME;
import static org.infinispan.server.configuration.security.LdapIdentityMappingConfiguration.RDN_IDENTIFIER;
import static org.infinispan.server.configuration.security.LdapIdentityMappingConfiguration.SEARCH_BASE_DN;
import static org.infinispan.server.configuration.security.LdapIdentityMappingConfiguration.SEARCH_TIME_LIMIT;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class LdapIdentityMappingConfigurationBuilder implements Builder<LdapIdentityMappingConfiguration> {
   private final AttributeSet attributes;
   private final List<LdapAttributeConfigurationBuilder> attributeMappings = new ArrayList<>();
   private final LdapUserPasswordMapperConfigurationBuilder userPasswordMapper;

   LdapIdentityMappingConfigurationBuilder(LdapRealmConfigurationBuilder ldapConfigurationBuilder) {
      this.userPasswordMapper = new LdapUserPasswordMapperConfigurationBuilder(ldapConfigurationBuilder);
      this.attributes = LdapIdentityMappingConfiguration.attributeDefinitionSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public LdapIdentityMappingConfigurationBuilder rdnIdentifier(String rdnIdentifier) {
      attributes.attribute(RDN_IDENTIFIER).set(rdnIdentifier);
      return this;
   }

   public LdapIdentityMappingConfigurationBuilder searchBaseDn(String searchBaseDn) {
      attributes.attribute(SEARCH_BASE_DN).set(searchBaseDn);
      return this;
   }

   public LdapIdentityMappingConfigurationBuilder searchRecursive(boolean searchRecursive) {
      attributes.attribute(SEARCH_RECURSIVE).set(searchRecursive);
      return this;
   }

   public LdapIdentityMappingConfigurationBuilder searchTimeLimit(int searchTimeLimit) {
      attributes.attribute(SEARCH_TIME_LIMIT).set(searchTimeLimit);
      return this;
   }

   public LdapIdentityMappingConfigurationBuilder filterName(String filterName) {
      attributes.attribute(FILTER_NAME).set(filterName);
      return this;
   }

   public LdapAttributeConfigurationBuilder addAttributeMapping() {
      LdapAttributeConfigurationBuilder builder = new LdapAttributeConfigurationBuilder();
      attributeMappings.add(builder);
      return builder;
   }

   public LdapUserPasswordMapperConfigurationBuilder userPasswordMapper() {
      return userPasswordMapper;
   }

   @Override
   public void validate() {
      attributeMappings.forEach(Builder::validate);
      userPasswordMapper.validate();
   }

   @Override
   public LdapIdentityMappingConfiguration create() {
      List<LdapAttributeConfiguration> mappingConfigurations = attributeMappings.stream()
            .map(LdapAttributeConfigurationBuilder::create).collect(Collectors.toList());

      return new LdapIdentityMappingConfiguration(attributes.protect(), mappingConfigurations, userPasswordMapper.create());
   }

   @Override
   public LdapIdentityMappingConfigurationBuilder read(LdapIdentityMappingConfiguration template, Combine combine) {
      attributes.read(template.attributes(), combine);
      attributeMappings.clear();
      template.attributeMappings().forEach(a -> addAttributeMapping().read(a, combine));
      userPasswordMapper.read(template.userPasswordMapper(), combine);
      return this;
   }
}
