package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.DistributedRealmConfiguration.NAME;

import java.util.function.Supplier;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.security.PasswordCredentialSource;
import org.wildfly.security.auth.realm.ldap.DirContextFactory;
import org.wildfly.security.auth.server.NameRewriter;
import org.wildfly.security.credential.source.CredentialSource;

/**
 * @since 10.0
 */
public class LdapRealmConfigurationBuilder implements RealmProviderBuilder<LdapRealmConfiguration> {
   private final AttributeSet attributes;
   private final LdapIdentityMappingConfigurationBuilder identityMapping;


   LdapRealmConfigurationBuilder() {
      this.attributes = LdapRealmConfiguration.attributeDefinitionSet();
      identityMapping = new LdapIdentityMappingConfigurationBuilder(this);
   }

   public LdapIdentityMappingConfigurationBuilder identityMapping() {
      return identityMapping;
   }

   public LdapRealmConfigurationBuilder name(String name) {
      attributes.attribute(LdapRealmConfiguration.NAME).set(name);
      return this;
   }

   @Override
   public String name() {
      return attributes.attribute(NAME).get();
   }

   public LdapRealmConfigurationBuilder url(String url) {
      attributes.attribute(LdapRealmConfiguration.URL).set(url);
      return this;
   }

   public LdapRealmConfigurationBuilder principal(String principal) {
      attributes.attribute(LdapRealmConfiguration.PRINCIPAL).set(principal);
      return this;
   }

   public LdapRealmConfigurationBuilder credential(char[] credential) {
      attributes.attribute(LdapRealmConfiguration.CREDENTIAL).set(new PasswordCredentialSource(credential));
      return this;
   }

   public LdapRealmConfigurationBuilder credential(Supplier<CredentialSource> credential) {
      attributes.attribute(LdapRealmConfiguration.CREDENTIAL).set(credential);
      return this;
   }

   public LdapRealmConfigurationBuilder directEvidenceVerification(boolean value) {
      attributes.attribute(LdapRealmConfiguration.DIRECT_EVIDENCE_VERIFICATION).set(value);
      return this;
   }

   public LdapRealmConfigurationBuilder pageSize(int value) {
      attributes.attribute(LdapRealmConfiguration.PAGE_SIZE).set(value);
      return this;
   }

   public LdapRealmConfigurationBuilder nameRewriter(NameRewriter rewriter) {
      attributes.attribute(LdapRealmConfiguration.NAME_REWRITER).set(rewriter);
      return this;
   }

   public LdapRealmConfigurationBuilder connectionTimeout(int connectionTimeout) {
      attributes.attribute(LdapRealmConfiguration.CONNECTION_TIMEOUT).set(connectionTimeout);
      return this;
   }

   public LdapRealmConfigurationBuilder readTimeout(int readTimeout) {
      attributes.attribute(LdapRealmConfiguration.READ_TIMEOUT).set(readTimeout);
      return this;
   }

   public LdapRealmConfigurationBuilder connectionPooling(boolean connectionPooling) {
      attributes.attribute(LdapRealmConfiguration.CONNECTION_POOLING).set(connectionPooling);
      return this;
   }

   public LdapRealmConfigurationBuilder referralMode(DirContextFactory.ReferralMode referralMode) {
      attributes.attribute(LdapRealmConfiguration.REFERRAL_MODE).set(referralMode);
      return this;
   }

   @Override
   public void validate() {
      identityMapping.validate();
   }

   @Override
   public LdapRealmConfiguration create() {
      return new LdapRealmConfiguration(attributes.protect(), identityMapping.create());
   }

   @Override
   public LdapRealmConfigurationBuilder read(LdapRealmConfiguration template) {
      attributes.read(template.attributes());
      identityMapping.read(template.identityMapping());
      return this;
   }

   @Override
   public int compareTo(RealmProviderBuilder o) {
      return 0; // Irrelevant
   }

   boolean isDirectVerificationEnabled() {
      Attribute<Boolean> attribute = attributes.attribute(LdapRealmConfiguration.DIRECT_EVIDENCE_VERIFICATION);
      return !attribute.isNull() && attribute.get();
   }
}
