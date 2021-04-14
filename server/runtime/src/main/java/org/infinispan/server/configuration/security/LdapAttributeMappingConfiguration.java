package org.infinispan.server.configuration.security;

import java.util.List;

/**
 * @since 10.0
 */
public class LdapAttributeMappingConfiguration {

   private final List<LdapAttributeConfiguration> attributesConfiguration;

   LdapAttributeMappingConfiguration(List<LdapAttributeConfiguration> attributesConfiguration) {
      this.attributesConfiguration = attributesConfiguration;
   }

   public List<LdapAttributeConfiguration> attributesConfiguration() {
      return attributesConfiguration;
   }
}
