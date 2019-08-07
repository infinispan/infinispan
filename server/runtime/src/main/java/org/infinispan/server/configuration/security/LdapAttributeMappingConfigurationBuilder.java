package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;

/**
 * @since 10.0
 */
public class LdapAttributeMappingConfigurationBuilder implements Builder<LdapAttributeMappingConfiguration> {

   private final List<LdapAttributeConfigurationBuilder> attributes = new ArrayList<>();
   private final LdapRealmConfigurationBuilder ldapConfigurationBuilder;

   LdapAttributeMappingConfigurationBuilder(LdapRealmConfigurationBuilder ldapConfigurationBuilder) {
      this.ldapConfigurationBuilder = ldapConfigurationBuilder;
   }

   public LdapAttributeConfigurationBuilder addAttribute() {
      LdapAttributeConfigurationBuilder attributeConfigurationBuilder = new LdapAttributeConfigurationBuilder(ldapConfigurationBuilder);
      attributes.add(attributeConfigurationBuilder);
      return attributeConfigurationBuilder;
   }

   @Override
   public LdapAttributeMappingConfiguration create() {
      List<LdapAttributeConfiguration> attributesConfiguration = attributes.stream()
            .map(LdapAttributeConfigurationBuilder::create).collect(Collectors.toList());
      return new LdapAttributeMappingConfiguration(attributesConfiguration);
   }

   @Override
   public void validate() {
   }

   @Override
   public LdapAttributeMappingConfigurationBuilder read(LdapAttributeMappingConfiguration template) {
      attributes.clear();
      template.attributesConfiguration().forEach(a -> addAttribute().read(a));
      return this;
   }
}
