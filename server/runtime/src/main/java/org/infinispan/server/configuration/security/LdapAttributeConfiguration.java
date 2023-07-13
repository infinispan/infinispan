package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.auth.realm.ldap.AttributeMapping;
import org.wildfly.security.auth.realm.ldap.LdapSecurityRealmBuilder;

/**
 * @since 10.0
 */
public class LdapAttributeConfiguration extends ConfigurationElement<LdapAttributeConfiguration> {
   static final AttributeDefinition<String> FILTER = AttributeDefinition.builder(Attribute.FILTER, null, String.class).build();
   static final AttributeDefinition<String> FILTER_DN = AttributeDefinition.builder(Attribute.FILTER_DN, null, String.class).build();
   static final AttributeDefinition<String> FROM = AttributeDefinition.builder(Attribute.FROM, null, String.class).build();
   static final AttributeDefinition<String> TO = AttributeDefinition.builder(Attribute.TO, null, String.class).build();
   static final AttributeDefinition<String> REFERENCE = AttributeDefinition.builder(Attribute.REFERENCE, null, String.class).build();
   static final AttributeDefinition<Boolean> SEARCH_RECURSIVE = AttributeDefinition.builder(Attribute.SEARCH_RECURSIVE, true, Boolean.class).build();
   static final AttributeDefinition<Integer> ROLE_RECURSION = AttributeDefinition.builder(Attribute.ROLE_RECURSION, 0, Integer.class).build();
   static final AttributeDefinition<String> ROLE_RECURSION_NAME = AttributeDefinition.builder(Attribute.ROLE_RECURSION_NAME, "cn", String.class).build();
   static final AttributeDefinition<String> EXTRACT_RDN = AttributeDefinition.builder(Attribute.EXTRACT_RDN, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapAttributeConfiguration.class, FILTER, FILTER_DN, FROM, TO, REFERENCE, SEARCH_RECURSIVE, ROLE_RECURSION, ROLE_RECURSION_NAME, EXTRACT_RDN);
   }

   LdapAttributeConfiguration(AttributeSet attributes) {
      super(attributes.attribute(REFERENCE).isModified() ? Element.ATTRIBUTE_REFERENCE : Element.ATTRIBUTE, attributes);
   }

   public void build(LdapSecurityRealmBuilder.IdentityMappingBuilder identity) {
      AttributeMapping.Builder builder = attributes.attribute(REFERENCE).isModified() ?
            AttributeMapping.fromReference(attributes.attribute(REFERENCE).get()) :
            AttributeMapping.fromFilter(attributes.attribute(FILTER).get());
      attributes.attribute(FILTER_DN).apply(builder::searchDn);
      attributes.attribute(FROM).apply(builder::from);
      attributes.attribute(TO).apply(builder::to);
      attributes.attribute(SEARCH_RECURSIVE).apply(builder::searchRecursively);
      attributes.attribute(ROLE_RECURSION).apply(builder::roleRecursion);
      attributes.attribute(ROLE_RECURSION_NAME).apply(builder::roleRecursionName);
      attributes.attribute(EXTRACT_RDN).apply(builder::extractRdn);
      identity.map(builder.build());
   }
}
