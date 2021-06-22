package org.infinispan.server.configuration.security;

import static org.infinispan.server.configuration.security.RealmConfiguration.CACHE_LIFESPAN;
import static org.infinispan.server.configuration.security.RealmConfiguration.CACHE_MAX_SIZE;

import java.security.GeneralSecurityException;
import java.util.EnumSet;
import java.util.function.Consumer;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.security.ServerSecurityRealm;
import org.infinispan.server.security.realm.CachingModifiableSecurityRealm;
import org.infinispan.server.security.realm.CachingSecurityRealm;
import org.wildfly.security.auth.permission.LoginPermission;
import org.wildfly.security.auth.realm.CacheableSecurityRealm;
import org.wildfly.security.auth.server.ModifiableSecurityRealm;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityRealm;
import org.wildfly.security.cache.LRURealmIdentityCache;
import org.wildfly.security.cache.RealmIdentityCache;
import org.wildfly.security.permission.PermissionVerifier;
import org.wildfly.security.ssl.SSLContextBuilder;

/**
 * @since 10.0
 */
public class RealmConfigurationBuilder implements Builder<RealmConfiguration> {
   private final AttributeSet attributes;

   private final SecurityDomain.Builder domainBuilder = SecurityDomain.builder();
   private final RealmsConfigurationBuilder realmsBuilder;

   private final ServerIdentitiesConfigurationBuilder serverIdentitiesConfiguration = new ServerIdentitiesConfigurationBuilder(this);
   private final FileSystemRealmConfigurationBuilder fileSystemConfiguration = new FileSystemRealmConfigurationBuilder(this);
   private final LdapRealmConfigurationBuilder ldapConfiguration = new LdapRealmConfigurationBuilder(this);
   private final LocalRealmConfigurationBuilder localConfiguration = new LocalRealmConfigurationBuilder();
   private final TokenRealmConfigurationBuilder tokenConfiguration = new TokenRealmConfigurationBuilder(this);
   private final TrustStoreRealmConfigurationBuilder trustStoreConfiguration = new TrustStoreRealmConfigurationBuilder(this);
   private final PropertiesRealmConfigurationBuilder propertiesRealmConfiguration = new PropertiesRealmConfigurationBuilder(this);

   private SSLContext sslContext = null;
   private SSLContextBuilder sslContextBuilder = null;
   private Supplier<Boolean> httpChallengeReadiness = () -> true;
   private ServerSecurityRealm serverSecurityRealm = null;
   private EnumSet<ServerSecurityRealm.Feature> features = EnumSet.noneOf(ServerSecurityRealm.Feature.class);
   private RealmIdentityCache cache;

   RealmConfigurationBuilder(String name, RealmsConfigurationBuilder realmsBuilder) {
      this.realmsBuilder = realmsBuilder;
      this.attributes = RealmConfiguration.attributeDefinitionSet();
      domainBuilder.setPermissionMapper((principal, roles) -> PermissionVerifier.from(new LoginPermission()));
      attributes.attribute(RealmConfiguration.NAME).set(name);
   }

   RealmsConfigurationBuilder realmsBuilder() {
      return realmsBuilder;
   }

   SSLContextBuilder sslContextBuilder() {
      if (sslContextBuilder == null) {
         sslContextBuilder = new SSLContextBuilder();
      }
      return sslContextBuilder;
   }

   SecurityDomain.Builder domainBuilder() {
      return domainBuilder;
   }

   public RealmConfigurationBuilder cacheMaxSize(int size) {
      this.attributes.attribute(CACHE_MAX_SIZE).set(size);
      return this;
   }

   public RealmConfigurationBuilder cacheLifespan(long lifespan) {
      this.attributes.attribute(CACHE_LIFESPAN).set(lifespan);
      return this;
   }

   public FileSystemRealmConfigurationBuilder fileSystemConfiguration() {
      return fileSystemConfiguration;
   }

   public LdapRealmConfigurationBuilder ldapConfiguration() {
      return ldapConfiguration;
   }

   public LocalRealmConfigurationBuilder localConfiguration() {
      return localConfiguration;
   }

   public TokenRealmConfigurationBuilder tokenConfiguration() {
      return tokenConfiguration;
   }

   public TrustStoreRealmConfigurationBuilder trustStoreConfiguration() {
      return trustStoreConfiguration;
   }

   public ServerIdentitiesConfigurationBuilder serverIdentitiesConfiguration() {
      return serverIdentitiesConfiguration;
   }

   public PropertiesRealmConfigurationBuilder propertiesRealm() {
      return propertiesRealmConfiguration;
   }

   void setHttpChallengeReadiness(Supplier<Boolean> readiness) {
      this.httpChallengeReadiness = readiness;
   }

   @Override
   public void validate() {
      fileSystemConfiguration.validate();
      ldapConfiguration.validate();
      localConfiguration.validate();
      tokenConfiguration.validate();
      trustStoreConfiguration.validate();
      serverIdentitiesConfiguration.validate();
      propertiesRealmConfiguration.validate();
   }

   @Override
   public RealmConfiguration create() {
      return new RealmConfiguration(
            attributes.protect(),
            fileSystemConfiguration.create(),
            ldapConfiguration.create(),
            localConfiguration.create(),
            tokenConfiguration.create(),
            trustStoreConfiguration.create(),
            serverIdentitiesConfiguration.create(),
            propertiesRealmConfiguration.create());
   }

   @Override
   public RealmConfigurationBuilder read(RealmConfiguration template) {
      this.attributes.read(template.attributes());
      fileSystemConfiguration.read(template.fileSystemConfiguration());
      ldapConfiguration.read(template.ldapConfiguration());
      localConfiguration.read(template.localConfiguration());
      tokenConfiguration.read(template.tokenConfiguration());
      trustStoreConfiguration.read(template.trustStoreConfiguration());
      serverIdentitiesConfiguration.read(template.serverIdentitiesConfiguration());
      propertiesRealmConfiguration.read(template.propertiesRealm());
      return this;
   }

   ServerSecurityRealm getServerSecurityRealm() {
      if (serverSecurityRealm == null) {
         SecurityDomain securityDomain = domainBuilder.build();
         String name = attributes.attribute(RealmConfiguration.NAME).get();
         serverSecurityRealm = new ServerSecurityRealm(name, securityDomain, httpChallengeReadiness, serverIdentitiesConfiguration.create(), features);
      }
      return serverSecurityRealm;
   }

   SSLContext getSSLContext() {
      if (sslContextBuilder == null) return null;
      if (sslContext == null) {
         if (features.contains(ServerSecurityRealm.Feature.TRUST)) {
            sslContextBuilder.setSecurityDomain(getServerSecurityRealm().getSecurityDomain());
         }
         sslContextBuilder.setWrap(false);
         String sslProvider = SslContextFactory.getDefaultSslProvider();
         if (!SslContextFactory.FALLBACK_SSL_PROVIDER.equals(sslProvider)) {
            sslContextBuilder.setProviderName(sslProvider);
         }
         try {
            sslContext = sslContextBuilder.build().create();
         } catch (GeneralSecurityException e) {
            throw new CacheConfigurationException(e);
         }
      }
      return sslContext;
   }

   public void addFeature(ServerSecurityRealm.Feature feature) {
      features.add(feature);
   }

   private SecurityRealm cacheable(SecurityRealm realm) {
      int maxEntries = attributes.attribute(CACHE_MAX_SIZE).get();
      if (maxEntries > 0 && realm instanceof CacheableSecurityRealm) {
         if (cache == null) {
            cache = new LRURealmIdentityCache(maxEntries, attributes.attribute(CACHE_LIFESPAN).get());
         }
         if (realm instanceof ModifiableSecurityRealm) {
            return new CachingModifiableSecurityRealm((CacheableSecurityRealm) realm, cache);
         } else {
            return new CachingSecurityRealm((CacheableSecurityRealm) realm, cache);
         }
      } else {
         return realm;
      }
   }

   public void addRealm(String realmName, SecurityRealm realm) {
      addRealm(realmName, realm, null);
   }

   public void addRealm(String realmName, SecurityRealm realm, Consumer<SecurityDomain.RealmBuilder> realmBuilderConsumer) {
      SecurityDomain.RealmBuilder realmBuilder = domainBuilder.addRealm(realmName, cacheable(realm));
      if (realmBuilderConsumer != null) {
         realmBuilderConsumer.accept(realmBuilder);
      }
      // Build with realm with the side-effect of adding itself to the parent
      realmBuilder.build();
      if (domainBuilder.getDefaultRealmName() == null) {
         domainBuilder.setDefaultRealmName(realmName);
      }
   }
}
