package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.LdapAttributeConfiguration.EXTRACT_RDN;
import static org.infinispan.server.configuration.security.LdapAttributeConfiguration.FILTER;
import static org.infinispan.server.configuration.security.LdapAttributeConfiguration.FILTER_DN;
import static org.infinispan.server.configuration.security.LdapAttributeConfiguration.FROM;
import static org.infinispan.server.configuration.security.LdapAttributeConfiguration.REFERENCE;
import static org.infinispan.server.configuration.security.LdapAttributeConfiguration.ROLE_RECURSION;
import static org.infinispan.server.configuration.security.LdapAttributeConfiguration.ROLE_RECURSION_NAME;
import static org.infinispan.server.configuration.security.LdapAttributeConfiguration.SEARCH_RECURSIVE;
import static org.infinispan.server.configuration.security.LdapAttributeConfiguration.TO;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * @since 10.0
 */
public class LdapAttributeConfigurationBuilder implements Builder<LdapAttributeConfiguration> {
   private final AttributeSet attributes;

   LdapAttributeConfigurationBuilder() {
      this.attributes = LdapAttributeConfiguration.attributeDefinitionSet();
   }

   public LdapAttributeConfigurationBuilder filter(String filter) {
      attributes.attribute(FILTER).set(filter);
      return this;
   }

   public LdapAttributeConfigurationBuilder reference(String reference) {
      attributes.attribute(REFERENCE).set(reference);
      return this;
   }

   public LdapAttributeConfigurationBuilder filterBaseDn(String filterBaseDn) {
      attributes.attribute(FILTER_DN).set(filterBaseDn);
      return this;
   }

   public LdapAttributeConfigurationBuilder from(String from) {
      attributes.attribute(FROM).set(from);
      return this;
   }

   public LdapAttributeConfigurationBuilder to(String to) {
      attributes.attribute(TO).set(to);
      return this;
   }

   public LdapAttributeConfigurationBuilder searchRecursive(boolean searchRecursive) {
      attributes.attribute(SEARCH_RECURSIVE).set(searchRecursive);

      return this;
   }

   public LdapAttributeConfigurationBuilder roleRecursion(int roleRecursion) {
      attributes.attribute(ROLE_RECURSION).set(roleRecursion);
      return this;
   }

   public LdapAttributeConfigurationBuilder roleRecursionName(String roleRecursionName) {
      attributes.attribute(ROLE_RECURSION_NAME).set(roleRecursionName);
      return this;
   }

   public LdapAttributeConfigurationBuilder extractRdn(String rdn) {
      attributes.attribute(EXTRACT_RDN).set(rdn);
      return this;
   }

   @Override
   public LdapAttributeConfiguration create() {
      return new LdapAttributeConfiguration(attributes.protect());
   }

   @Override
   public LdapAttributeConfigurationBuilder read(LdapAttributeConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }
}
