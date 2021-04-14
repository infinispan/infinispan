package org.infinispan.server.configuration.security;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSerializer;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.wildfly.security.auth.realm.ldap.DirContextFactory;
import org.wildfly.security.auth.server.NameRewriter;

/**
 * @since 10.0
 */
public class LdapRealmConfiguration extends ConfigurationElement<LdapRealmConfiguration> {

   static final AttributeDefinition<char[]> CREDENTIAL = AttributeDefinition.builder(Attribute.CREDENTIAL, null, char[].class)
         .serializer(AttributeSerializer.SECRET)
         .build();
   static final AttributeDefinition<Boolean> DIRECT_EVIDENCE_VERIFICATION = AttributeDefinition.builder(Attribute.DIRECT_VERIFICATION, null, Boolean.class).build();
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<NameRewriter> NAME_REWRITER = AttributeDefinition.builder(Element.NAME_REWRITER, null, NameRewriter.class).autoPersist(false).build();
   static final AttributeDefinition<String> PRINCIPAL = AttributeDefinition.builder(Attribute.PRINCIPAL, null, String.class).build();
   static final AttributeDefinition<Integer> PAGE_SIZE = AttributeDefinition.builder(Attribute.PAGE_SIZE, 50, Integer.class).build();
   static final AttributeDefinition<String> RDN_IDENTIFIER = AttributeDefinition.builder(Attribute.RDN_IDENTIFIER, null, String.class).build();
   static final AttributeDefinition<String> SEARCH_DN = AttributeDefinition.builder(Attribute.SEARCH_DN, null, String.class).build();
   static final AttributeDefinition<String> URL = AttributeDefinition.builder(Attribute.URL, null, String.class).build();
   static final AttributeDefinition<Integer> CONNECTION_TIMEOUT = AttributeDefinition.builder(Attribute.CONNECTION_TIMEOUT, 5_000, Integer.class).build();
   static final AttributeDefinition<Integer> READ_TIMEOUT = AttributeDefinition.builder(Attribute.READ_TIMEOUT, 60_000, Integer.class).build();
   static final AttributeDefinition<Boolean> CONNECTION_POOLING = AttributeDefinition.builder(Attribute.CONNECTION_POOLING, false, Boolean.class).build();
   static final AttributeDefinition<DirContextFactory.ReferralMode> REFERRAL_MODE = AttributeDefinition.builder(Attribute.REFERRAL_MODE, DirContextFactory.ReferralMode.IGNORE, DirContextFactory.ReferralMode.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapRealmConfiguration.class, CREDENTIAL, DIRECT_EVIDENCE_VERIFICATION, NAME, NAME_REWRITER, PRINCIPAL, PAGE_SIZE, RDN_IDENTIFIER, SEARCH_DN, URL, CONNECTION_TIMEOUT, READ_TIMEOUT, CONNECTION_POOLING, REFERRAL_MODE);
   }

   private final List<LdapIdentityMappingConfiguration> identityMappings;

   LdapRealmConfiguration(AttributeSet attributes, List<LdapIdentityMappingConfiguration> identityMappings) {
      super(Element.LDAP_REALM, attributes);
      this.identityMappings = identityMappings;
   }

   public List<LdapIdentityMappingConfiguration> identityMappings() {
      return identityMappings;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public NameRewriter nameRewriter() {
      return attributes.attribute(NAME_REWRITER).get();
   }
}
