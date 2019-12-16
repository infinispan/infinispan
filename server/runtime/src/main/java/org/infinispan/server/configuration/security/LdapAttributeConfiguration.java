package org.infinispan.server.configuration.security;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;

/**
 * @since 10.0
 */
public class LdapAttributeConfiguration implements ConfigurationInfo {
   static final AttributeDefinition<String> FILTER = AttributeDefinition.builder("filter", null, String.class).build();
   static final AttributeDefinition<String> FILTER_DN = AttributeDefinition.builder("filterDn", null, String.class).build();
   static final AttributeDefinition<String> FROM = AttributeDefinition.builder("from", null, String.class).build();
   static final AttributeDefinition<String> TO = AttributeDefinition.builder("to", null, String.class).build();
   static final AttributeDefinition<String> REFERENCE = AttributeDefinition.builder("reference", null, String.class).build();
   static final AttributeDefinition<Boolean> SEARCH_RECURSIVE = AttributeDefinition.builder("searchRecursive", true, Boolean.class).build();
   static final AttributeDefinition<Integer> ROLE_RECURSION = AttributeDefinition.builder("roleRecursion", 0, Integer.class).build();
   static final AttributeDefinition<String> ROLE_RECURSION_NAME = AttributeDefinition.builder("roleRecursionName", "cn", String.class).build();
   static final AttributeDefinition<String> EXTRACT_RDN = AttributeDefinition.builder("extractRdn", null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapAttributeConfiguration.class, FILTER, FILTER_DN, FROM, TO, REFERENCE, SEARCH_RECURSIVE, ROLE_RECURSION, ROLE_RECURSION_NAME, EXTRACT_RDN);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.ATTRIBUTE.toString());

   private final AttributeSet attributes;

   LdapAttributeConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }
}
