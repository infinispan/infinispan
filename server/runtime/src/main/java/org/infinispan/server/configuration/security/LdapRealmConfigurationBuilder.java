package org.infinispan.server.configuration.security;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.wildfly.security.auth.realm.ldap.DirContextFactory;
import org.wildfly.security.auth.realm.ldap.LdapSecurityRealmBuilder;
import org.wildfly.security.auth.realm.ldap.SimpleDirContextFactoryBuilder;
import org.wildfly.security.auth.server.NameRewriter;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;

/**
 * @since 10.0
 */
public class LdapRealmConfigurationBuilder implements Builder<LdapRealmConfiguration> {
   private final AttributeSet attributes;
   private final List<LdapIdentityMappingConfigurationBuilder> identityMappings = new ArrayList<>();
   private final RealmConfigurationBuilder realmBuilder;
   private SecurityRealm securityRealm;
   private final SimpleDirContextFactoryBuilder dirContextBuilder = SimpleDirContextFactoryBuilder.builder();
   private final LdapSecurityRealmBuilder ldapRealmBuilder = LdapSecurityRealmBuilder.builder();
   private final LdapSecurityRealmBuilder.IdentityMappingBuilder identityMappingBuilder = ldapRealmBuilder.identityMapping();


   LdapRealmConfigurationBuilder(RealmConfigurationBuilder realmBuilder) {
      this.realmBuilder = realmBuilder;
      this.attributes = LdapRealmConfiguration.attributeDefinitionSet();
   }

   public SimpleDirContextFactoryBuilder getDirContextBuilder() {
      return dirContextBuilder;
   }

   LdapSecurityRealmBuilder getLdapRealmBuilder() {
      return ldapRealmBuilder;
   }

   LdapSecurityRealmBuilder.IdentityMappingBuilder getIdentityMappingBuilder() {
      return identityMappingBuilder;
   }

   public LdapIdentityMappingConfigurationBuilder addIdentityMap() {
      LdapIdentityMappingConfigurationBuilder identity = new LdapIdentityMappingConfigurationBuilder(this);
      identityMappings.add(identity);
      return identity;
   }

   public LdapRealmConfigurationBuilder name(String name) {
      attributes.attribute(LdapRealmConfiguration.NAME).set(name);
      return this;
   }

   public LdapRealmConfigurationBuilder url(String url) {
      attributes.attribute(LdapRealmConfiguration.URL).set(url);
      dirContextBuilder.setProviderUrl(url);
      return this;
   }

   public LdapRealmConfigurationBuilder principal(String principal) {
      attributes.attribute(LdapRealmConfiguration.PRINCIPAL).set(principal);
      dirContextBuilder.setSecurityPrincipal(principal);
      return this;
   }


   public LdapRealmConfigurationBuilder credential(String credential) {
      attributes.attribute(LdapRealmConfiguration.CREDENTIAL).set(credential);
      dirContextBuilder.setSecurityCredential(credential);
      return this;
   }

   public LdapRealmConfigurationBuilder directEvidenceVerification(boolean value) {
      attributes.attribute(LdapRealmConfiguration.DIRECT_EVIDENCE_VERIFICATION).set(value);
      ldapRealmBuilder.addDirectEvidenceVerification(value);
      return this;
   }

   public LdapRealmConfigurationBuilder pageSize(int value) {
      attributes.attribute(LdapRealmConfiguration.PAGE_SIZE).set(value);
      ldapRealmBuilder.setPageSize(value);
      return this;
   }

   public LdapRealmConfigurationBuilder searchDn(String value) {
      attributes.attribute(LdapRealmConfiguration.SEARCH_DN).set(value);
      identityMappingBuilder.setSearchDn(value);
      return this;
   }

   public LdapRealmConfigurationBuilder rdnIdentifier(String value) {
      attributes.attribute(LdapRealmConfiguration.RDN_IDENTIFIER).set(value);
      identityMappingBuilder.setRdnIdentifier(value);
      return this;
   }

   public LdapRealmConfigurationBuilder nameRewriter(NameRewriter rewriter) {
      attributes.attribute(LdapRealmConfiguration.NAME_REWRITER).set(rewriter);
      return this;
   }

   @Override
   public void validate() {
      identityMappings.forEach(LdapIdentityMappingConfigurationBuilder::validate);
   }

   @Override
   public LdapRealmConfiguration create() {
      List<LdapIdentityMappingConfiguration> identities = identityMappings.stream()
            .map(LdapIdentityMappingConfigurationBuilder::create).collect(Collectors.toList());
      return new LdapRealmConfiguration(attributes.protect(), identities);
   }

   @Override
   public LdapRealmConfigurationBuilder read(LdapRealmConfiguration template) {
      attributes.read(template.attributes());
      identityMappings.clear();
      template.identityMappings().forEach(i -> addIdentityMap().read(i));
      return this;
   }

   public SecurityRealm build() {
      if (securityRealm == null) {
         identityMappingBuilder.build();
         DirContextFactory dirContextFactory = dirContextBuilder.build();
         ldapRealmBuilder.setDirContextSupplier(() -> dirContextFactory.obtainDirContext(DirContextFactory.ReferralMode.FOLLOW));
         if (attributes.attribute(LdapRealmConfiguration.NAME_REWRITER).isModified()) {
            ldapRealmBuilder.setNameRewriter(attributes.attribute(LdapRealmConfiguration.NAME_REWRITER).get());
         }
         String name = attributes.attribute(LdapRealmConfiguration.NAME).get();
         SecurityDomain.Builder domainBuilder = realmBuilder.domainBuilder();

         securityRealm = ldapRealmBuilder.build();
         domainBuilder.addRealm(name, securityRealm).build();
         if (domainBuilder.getDefaultRealmName() == null) {
            domainBuilder.setDefaultRealmName(name);
         }
      }
      return securityRealm;
   }
}
