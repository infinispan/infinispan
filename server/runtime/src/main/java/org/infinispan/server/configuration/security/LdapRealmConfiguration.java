package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ClassAttributeSerializer;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.configuration.PasswordSerializer;
import org.wildfly.security.auth.realm.ldap.DirContextFactory;
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
   static final AttributeDefinition<NameRewriter> NAME_REWRITER = AttributeDefinition.builder("nameRewriter", null, NameRewriter.class).serializer(ClassAttributeSerializer.INSTANCE).build();
   static final AttributeDefinition<String> PRINCIPAL = AttributeDefinition.builder("principal", null, String.class).build();
   static final AttributeDefinition<Integer> PAGE_SIZE = AttributeDefinition.builder("pageSize", 50, Integer.class).build();
   static final AttributeDefinition<String> RDN_IDENTIFIER = AttributeDefinition.builder("rdnIdentifier", null, String.class).build();
   static final AttributeDefinition<String> SEARCH_DN = AttributeDefinition.builder("searchDn", null, String.class).build();
   static final AttributeDefinition<String> URL = AttributeDefinition.builder("url", null, String.class).build();
   static final AttributeDefinition<Integer> CONNECTION_TIMEOUT = AttributeDefinition.builder("connectionTimeout", 5_000, Integer.class).build();
   static final AttributeDefinition<Integer> READ_TIMEOUT = AttributeDefinition.builder("readTimeout", 60_000, Integer.class).build();
   static final AttributeDefinition<Boolean> CONNECTION_POOLING = AttributeDefinition.builder("connectionPool", false, Boolean.class).build();
   static final AttributeDefinition<DirContextFactory.ReferralMode> REFERRAL_MODE = AttributeDefinition.builder("referralMode", DirContextFactory.ReferralMode.IGNORE, DirContextFactory.ReferralMode.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapRealmConfiguration.class, CREDENTIAL, DIRECT_EVIDENCE_VERIFICATION, NAME, NAME_REWRITER, PRINCIPAL, PAGE_SIZE, RDN_IDENTIFIER, SEARCH_DN, URL, CONNECTION_TIMEOUT, READ_TIMEOUT, CONNECTION_POOLING, REFERRAL_MODE);
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
