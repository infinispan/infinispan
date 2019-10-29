package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.auth.server.NameRewriter;

/**
 * @since 10.0
 */
public class LdapRealmConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<String> CREDENTIAL = AttributeDefinition.builder("credential", null, String.class)
         .serializer(PasswordSerializer.INSTANCE)
         .build();
   static final AttributeDefinition<Boolean> DIRECT_EVIDENCE_VERIFICATION = AttributeDefinition.builder("directEvidenceVerification", null, Boolean.class).build();
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<NameRewriter> NAME_REWRITER = AttributeDefinition.builder("nameRewriter", null, NameRewriter.class).build();
   static final AttributeDefinition<String> PRINCIPAL = AttributeDefinition.builder("principal", null, String.class).build();
   static final AttributeDefinition<Integer> PAGE_SIZE = AttributeDefinition.builder("pageSize", null, Integer.class).build();
   static final AttributeDefinition<String> RDN_IDENTIFIER = AttributeDefinition.builder("rdnIdentifier", null, String.class).build();
   static final AttributeDefinition<String> SEARCH_DN = AttributeDefinition.builder("searchDn", null, String.class).build();
   static final AttributeDefinition<String> URL = AttributeDefinition.builder("url", null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapRealmConfiguration.class, CREDENTIAL, DIRECT_EVIDENCE_VERIFICATION, NAME, NAME_REWRITER, PRINCIPAL, PAGE_SIZE, RDN_IDENTIFIER, SEARCH_DN, URL);
   }

   private static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.LDAP_REALM.toString());

   private final AttributeSet attributes;
   private final List<LdapIdentityMappingConfiguration> identityMappings;
   private final List<ConfigurationInfo> subElements = new ArrayList<>();

   LdapRealmConfiguration(AttributeSet attributes, List<LdapIdentityMappingConfiguration> identityMappings) {
      this.attributes = attributes.checkProtection();
      this.identityMappings = identityMappings;
      this.subElements.addAll(identityMappings);
   }

   List<LdapIdentityMappingConfiguration> identityMappings() {
      return identityMappings;
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
