package org.infinispan.server.configuration.security;

import java.util.Properties;
import java.util.function.Supplier;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.configuration.ServerConfigurationSerializer;
import org.infinispan.server.security.ServerSecurityRealm;
import org.wildfly.security.auth.realm.ldap.DirContextFactory;
import org.wildfly.security.auth.realm.ldap.LdapSecurityRealmBuilder;
import org.wildfly.security.auth.realm.ldap.SimpleDirContextFactoryBuilder;
import org.wildfly.security.auth.server.NameRewriter;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.credential.source.CredentialSource;

/**
 * @since 10.0
 */
@BuiltBy(LdapRealmConfigurationBuilder.class)
public class LdapRealmConfiguration extends ConfigurationElement<LdapRealmConfiguration> implements RealmProvider {

   static final AttributeDefinition<Supplier<CredentialSource>> CREDENTIAL = AttributeDefinition.builder(Attribute.CREDENTIAL, null, (Class<Supplier<CredentialSource>>) (Class<?>) char[].class)
         .serializer(ServerConfigurationSerializer.CREDENTIAL)
         .immutable()
         .build();
   static final AttributeDefinition<Boolean> DIRECT_EVIDENCE_VERIFICATION = AttributeDefinition.builder(Attribute.DIRECT_VERIFICATION, null, Boolean.class).immutable().build();
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, "ldap", String.class).immutable().build();
   static final AttributeDefinition<NameRewriter> NAME_REWRITER = AttributeDefinition.builder(Element.NAME_REWRITER, null, NameRewriter.class).autoPersist(false).immutable().build();
   static final AttributeDefinition<String> PRINCIPAL = AttributeDefinition.builder(Attribute.PRINCIPAL, null, String.class).immutable().build();
   static final AttributeDefinition<Integer> PAGE_SIZE = AttributeDefinition.builder(Attribute.PAGE_SIZE, 50, Integer.class).immutable().build();
   static final AttributeDefinition<String> URL = AttributeDefinition.builder(Attribute.URL, null, String.class).immutable().build();
   static final AttributeDefinition<Integer> CONNECTION_TIMEOUT = AttributeDefinition.builder(Attribute.CONNECTION_TIMEOUT, 5_000, Integer.class).immutable().build();
   static final AttributeDefinition<Integer> READ_TIMEOUT = AttributeDefinition.builder(Attribute.READ_TIMEOUT, 60_000, Integer.class).immutable().build();
   static final AttributeDefinition<Boolean> CONNECTION_POOLING = AttributeDefinition.builder(Attribute.CONNECTION_POOLING, false, Boolean.class).immutable().build();
   static final AttributeDefinition<DirContextFactory.ReferralMode> REFERRAL_MODE = AttributeDefinition.builder(Attribute.REFERRAL_MODE, DirContextFactory.ReferralMode.IGNORE, DirContextFactory.ReferralMode.class).immutable().build();
   static final AttributeDefinition<String> CLIENT_SSL_CONTEXT = AttributeDefinition.builder(Attribute.CLIENT_SSL_CONTEXT, null, String.class).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(LdapRealmConfiguration.class, DIRECT_EVIDENCE_VERIFICATION, NAME, NAME_REWRITER, PRINCIPAL, PAGE_SIZE, URL, CONNECTION_TIMEOUT, READ_TIMEOUT, CONNECTION_POOLING, REFERRAL_MODE, CLIENT_SSL_CONTEXT, CREDENTIAL);
   }

   private final LdapIdentityMappingConfiguration identityMapping;

   LdapRealmConfiguration(AttributeSet attributes, LdapIdentityMappingConfiguration identityMapping) {
      super(Element.LDAP_REALM, attributes);
      this.identityMapping = identityMapping;
   }

   public LdapIdentityMappingConfiguration identityMapping() {
      return identityMapping;
   }

   @Override
   public String name() {
      return attributes.attribute(NAME).get();
   }

   public NameRewriter nameRewriter() {
      return attributes.attribute(NAME_REWRITER).get();
   }

   @Override
   public SecurityRealm build(SecurityConfiguration security, RealmConfiguration realm, SecurityDomain.Builder domainBuilder, Properties properties) {
      LdapSecurityRealmBuilder ldapRealmBuilder = LdapSecurityRealmBuilder.builder();
      attributes.attribute(DIRECT_EVIDENCE_VERIFICATION).apply(ldapRealmBuilder::addDirectEvidenceVerification);
      ldapRealmBuilder.setPageSize(attributes.attribute(PAGE_SIZE).get());
      identityMapping.build(ldapRealmBuilder, realm);
      Properties connectionProperties = new Properties();
      connectionProperties.setProperty("com.sun.jndi.ldap.connect.pool", attributes.attribute(LdapRealmConfiguration.CONNECTION_POOLING).get().toString());
      SimpleDirContextFactoryBuilder dirContextBuilder = SimpleDirContextFactoryBuilder.builder();
      dirContextBuilder.setProviderUrl(attributes.attribute(URL).get());
      dirContextBuilder.setSecurityPrincipal(attributes.attribute(PRINCIPAL).get());
      dirContextBuilder.setCredentialSource(attributes.attribute(CREDENTIAL).get().get());
      dirContextBuilder
            .setConnectTimeout(attributes.attribute(LdapRealmConfiguration.CONNECTION_TIMEOUT).get())
            .setReadTimeout(attributes.attribute(LdapRealmConfiguration.READ_TIMEOUT).get());
      dirContextBuilder.setConnectionProperties(connectionProperties);
      attributes.attribute(CLIENT_SSL_CONTEXT).apply(v -> dirContextBuilder.setSocketFactory(security.realms().getRealm(v).clientSSLContext().getSocketFactory()));
      DirContextFactory dirContextFactory = dirContextBuilder.build();
      ldapRealmBuilder.setDirContextSupplier(() -> dirContextFactory.obtainDirContext(attributes.attribute(LdapRealmConfiguration.REFERRAL_MODE).get()));
      if (attributes.attribute(LdapRealmConfiguration.NAME_REWRITER).isModified()) {
         ldapRealmBuilder.setNameRewriter(attributes.attribute(LdapRealmConfiguration.NAME_REWRITER).get());
      }
      realm.addFeature(ServerSecurityRealm.Feature.PASSWORD_PLAIN);
      return ldapRealmBuilder.build();
   }

}
