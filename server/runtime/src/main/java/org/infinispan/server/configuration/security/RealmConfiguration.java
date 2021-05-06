package org.infinispan.server.configuration.security;

import java.security.GeneralSecurityException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
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
public class RealmConfiguration extends ConfigurationElement<RealmConfiguration> {
   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<String> DEFAULT_REALM = AttributeDefinition.builder(Attribute.DEFAULT_REALM, null, String.class).immutable().build();
   static final AttributeDefinition<Integer> CACHE_MAX_SIZE = AttributeDefinition.builder(Attribute.CACHE_MAX_SIZE, 256).build();
   static final AttributeDefinition<Long> CACHE_LIFESPAN = AttributeDefinition.builder(Attribute.CACHE_LIFESPAN, -1l).build();
   private EnumSet<ServerSecurityRealm.Feature> features = EnumSet.noneOf(ServerSecurityRealm.Feature.class);
   Map<String, SecurityRealm> realms; // visible to DistributedRealmConfiguration

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RealmConfiguration.class, NAME, DEFAULT_REALM, CACHE_MAX_SIZE, CACHE_LIFESPAN);
   }

   private final ServerIdentitiesConfiguration serverIdentitiesConfiguration;
   private final List<RealmProvider> realmProviders;

   private Supplier<Boolean> httpChallengeReadiness = () -> true;
   private ServerSecurityRealm serverSecurityRealm;
   private RealmIdentityCache cache;
   private SSLContext serverSslContext = null;
   private SSLContext clientSslContext = null;

   RealmConfiguration(AttributeSet attributes,
                      ServerIdentitiesConfiguration serverIdentitiesConfiguration,
                      List<RealmProvider> realmConfigurations) {
      super(Element.SECURITY_REALM, attributes);
      this.serverIdentitiesConfiguration = serverIdentitiesConfiguration;
      this.realmProviders = realmConfigurations;
   }

   public ServerIdentitiesConfiguration serverIdentitiesConfiguration() {
      return serverIdentitiesConfiguration;
   }

   public List<RealmProvider> realmProviders() {
      return realmProviders;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   @Override
   public String toString() {
      return "RealmConfiguration{" +
            "attributes=" + attributes +
            ", serverIdentitiesConfiguration=" + serverIdentitiesConfiguration +
            ", realmsConfiguration=" + realmProviders +
            '}';
   }

   public ServerSecurityRealm serverSecurityRealm() {
      if (serverSecurityRealm == null) {
         throw new IllegalStateException();
      }
      return serverSecurityRealm;
   }

   public SSLContext serverSSLContext() {
      if (serverSslContext == null) {
         throw Server.log.noSSLContext(name());
      } else {
         return serverSslContext;
      }
   }

   public SSLContext clientSSLContext() {
      if (clientSslContext == null) {
         throw Server.log.noSSLContext(name());
      } else {
         return clientSslContext;
      }
   }

   void init(SecurityConfiguration security, Properties properties) {
      SSLConfiguration sslConfiguration = serverIdentitiesConfiguration.sslConfiguration();
      SSLContextBuilder sslContextBuilder = sslConfiguration != null ? sslConfiguration.build(properties, features) : null;

      SecurityDomain.Builder domainBuilder = SecurityDomain.builder();
      domainBuilder.setPermissionMapper((principal, roles) -> PermissionVerifier.from(new LoginPermission()));

      if (realmProviders.isEmpty() || !(realmProviders.get(0) instanceof TrustStoreRealmConfiguration)) {
         // Initialize the SSLContexts now, because they may be needed for client connections of the LDAP or Token realms
         buildSSLContexts(sslContextBuilder);
      }
      realms = new HashMap<>(realmProviders.size());
      for(RealmProvider provider : realmProviders) {
         SecurityRealm realm = provider.build(security, this, domainBuilder, properties);
         realms.put(provider.name(), realm);
         if (realm != null) {
            domainBuilder.addRealm(provider.name(), cacheable(realm)).build();
            if (domainBuilder.getDefaultRealmName() == null) {
               domainBuilder.setDefaultRealmName(provider.name());
            }
         }
      }

      SecurityDomain securityDomain = domainBuilder.build();
      if (features.contains(ServerSecurityRealm.Feature.TRUST)) {
         sslContextBuilder.setSecurityDomain(securityDomain);
         // Initialize the SSLContexts
         buildSSLContexts(sslContextBuilder);
      }
      String name = attributes.attribute(RealmConfiguration.NAME).get();
      serverSecurityRealm = new ServerSecurityRealm(name, securityDomain, httpChallengeReadiness, serverIdentitiesConfiguration, features);
   }

   private void buildSSLContexts(SSLContextBuilder sslContextBuilder) {
      try {
         if (sslContextBuilder != null) {
            serverSslContext = sslContextBuilder.setClientMode(false).build().create();
            clientSslContext = sslContextBuilder.setClientMode(true).build().create();
         }
      } catch (GeneralSecurityException e) {
         throw new CacheConfigurationException(e);
      }
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

   void addFeature(ServerSecurityRealm.Feature feature) {
      this.features.add(feature);
   }

   public boolean hasFeature(ServerSecurityRealm.Feature feature) {
      return features.contains(feature);
   }

   void setHttpChallengeReadiness(Supplier<Boolean> httpChallengeReadiness) {
      this.httpChallengeReadiness = httpChallengeReadiness;
   }
}
