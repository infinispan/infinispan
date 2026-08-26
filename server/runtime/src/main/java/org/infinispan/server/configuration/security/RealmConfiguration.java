package org.infinispan.server.configuration.security;

import java.security.GeneralSecurityException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.commons.util.TimeQuantity;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.Attribute;
import org.infinispan.server.configuration.Element;
import org.infinispan.server.logging.Log;
import org.infinispan.server.security.ServerSecurityRealm;
import org.infinispan.server.security.realm.CachingModifiableSecurityRealm;
import org.infinispan.server.security.realm.CachingSecurityRealm;
import org.wildfly.security.auth.permission.LoginPermission;
import org.wildfly.security.auth.realm.BruteForceRealmWrapper;
import org.wildfly.security.auth.realm.CacheableSecurityRealm;
import org.wildfly.security.auth.server.EvidenceDecoder;
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
   private static final String BRUTE_FORCE_ENABLED = "infinispan.server.realm.%s.brute-force.enabled";
   private static final String BRUTE_FORCE_MAX_FAILED_ATTEMPTS = "infinispan.server.realm.%s.brute-force.max-failed-attempts";
   private static final String BRUTE_FORCE_LOCKOUT_INTERVAL = "infinispan.server.realm.%s.brute-force.lockout-interval";
   private static final String BRUTE_FORCE_SESSION_TIMEOUT = "infinispan.server.realm.%s.brute-force.session-timeout";
   private static final String BRUTE_FORCE_MAX_CACHED_SESSIONS = "infinispan.server.realm.%s.brute-force.max-cached-sessions";

   private static volatile ScheduledExecutorService bruteForceExecutor;

   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<String> DEFAULT_REALM = AttributeDefinition.builder(Attribute.DEFAULT_REALM, null, String.class).immutable().build();
   static final AttributeDefinition<Integer> CACHE_MAX_SIZE = AttributeDefinition.builder(Attribute.CACHE_MAX_SIZE, 256).build();
   static final AttributeDefinition<TimeQuantity> CACHE_LIFESPAN = AttributeDefinition.builder(Attribute.CACHE_LIFESPAN, TimeQuantity.valueOf("1m")).build();
   static final AttributeDefinition<EvidenceDecoder> EVIDENCE_DECODER = AttributeDefinition.builder(Attribute.EVIDENCE_DECODER, null, EvidenceDecoder.class).immutable().build();
   private final EnumSet<ServerSecurityRealm.Feature> features = EnumSet.noneOf(ServerSecurityRealm.Feature.class);
   Map<String, SecurityRealm> realms; // visible to DistributedRealmConfiguration

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RealmConfiguration.class, NAME, DEFAULT_REALM, CACHE_MAX_SIZE, CACHE_LIFESPAN, EVIDENCE_DECODER);
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
      super(Element.SECURITY_REALM, attributes, realmConfigurations.stream().map(p -> (ConfigurationElement) p).toArray(ConfigurationElement[]::new));
      this.serverIdentitiesConfiguration = serverIdentitiesConfiguration;
      this.realmProviders = realmConfigurations;
   }

   public ServerIdentitiesConfiguration serverIdentitiesConfiguration() {
      return serverIdentitiesConfiguration;
   }

   public List<RealmProvider> realmProviders() {
      return realmProviders;
   }

   public Map<String, SecurityRealm> realms() {
      return realms;
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

   public boolean hasServerSSLContext() {
      return serverSslContext != null;
   }

   public boolean hasClientSSLContext() {
      return clientSslContext != null;
   }

   void initSSLContexts(Properties properties) {
      SSLConfiguration sslConfiguration = serverIdentitiesConfiguration.sslConfiguration();
      SSLContextBuilder sslContextBuilder = sslConfiguration != null ? sslConfiguration.build(properties, features) : null;

      if (realmProviders.isEmpty() || !(realmProviders.get(0) instanceof TrustStoreRealmConfiguration)) {
         // Initialize the SSLContexts now, because they may be needed for client connections of the LDAP or Token realms
         buildSSLContexts(sslContextBuilder);
      }
   }

   void init(SecurityConfiguration security, Properties properties) {
      SecurityDomain.Builder domainBuilder = SecurityDomain.builder();
      attributes.attribute(EVIDENCE_DECODER).apply(domainBuilder::setEvidenceDecoder);
      domainBuilder.setPermissionMapper((principal, roles) -> PermissionVerifier.from(new LoginPermission()));

      String realmName = attributes.attribute(NAME).get();
      realms = new HashMap<>(realmProviders.size());
      String localRealmName = null;
      for (RealmProvider provider : realmProviders) {
         SecurityRealm realm = provider.build(security, this, domainBuilder, properties);
         provider.applyFeatures(features);
         realms.put(provider.name(), realm);
         if (realm != null) {
            SecurityRealm realmForDomain = cacheable(realm);
            if (isBruteForceProtectionEnabled(provider, realmName)) {
               realmForDomain = addBruteForceProtection(realmForDomain, realmName);
            }
            domainBuilder.addRealm(provider.name(), realmForDomain).build();
            if (domainBuilder.getDefaultRealmName() == null && !(provider instanceof LocalRealmConfiguration)) {
               domainBuilder.setDefaultRealmName(provider.name());
            }
         }
         if (provider instanceof LocalRealmConfiguration) {
            localRealmName = provider.name();
         }
      }
      // If no default realm was set (e.g. only local realm present), use the local realm
      if (domainBuilder.getDefaultRealmName() == null && localRealmName != null) {
         domainBuilder.setDefaultRealmName(localRealmName);
      }
      // Route the $local principal to the local realm so LOCALUSER auth works
      // regardless of which realm is the default
      if (localRealmName != null) {
         String localRealm = localRealmName;
         domainBuilder.setRealmMapper((principal, evidence) ->
               "$local".equals(principal.getName()) ? localRealm : null);
      }

      SecurityDomain securityDomain = domainBuilder.build();
      if (features.contains(ServerSecurityRealm.Feature.TRUST)) {
         SSLConfiguration sslConfiguration = serverIdentitiesConfiguration.sslConfiguration();
         SSLContextBuilder sslContextBuilder = sslConfiguration != null ? sslConfiguration.build(properties, features).setSecurityDomain(securityDomain) : null;
         // Initialize the SSLContexts
         buildSSLContexts(sslContextBuilder);
      }
      serverSecurityRealm = new ServerSecurityRealm(realmName, securityDomain, httpChallengeReadiness, serverIdentitiesConfiguration, features);
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

   private static boolean isBruteForceProtectionEnabled(RealmProvider provider, String realmName) {
      if (provider instanceof LocalRealmConfiguration ||
            provider instanceof TrustStoreRealmConfiguration ||
            provider instanceof TokenRealmConfiguration ||
            provider instanceof AggregateRealmConfiguration) {
         return false;
      }
      boolean enabled = Boolean.parseBoolean(
            System.getProperty(String.format(BRUTE_FORCE_ENABLED, realmName), "true"));
      if (!enabled) {
         Log.SERVER.bruteForceProtectionDisabled(realmName);
      }
      return enabled;
   }

   private static SecurityRealm addBruteForceProtection(SecurityRealm realm, String realmName) {
      BruteForceRealmWrapper wrapper = BruteForceRealmWrapper.create()
            .wrapping(realm)
            .withExecutor(getBruteForceExecutor())
            .setRealmName(realmName)
            .setMaxFailedAttempts(Integer.getInteger(String.format(BRUTE_FORCE_MAX_FAILED_ATTEMPTS, realmName), -1))
            .setLockoutInterval(Integer.getInteger(String.format(BRUTE_FORCE_LOCKOUT_INTERVAL, realmName), -1))
            .setFailureSessionTimeout(Integer.getInteger(String.format(BRUTE_FORCE_SESSION_TIMEOUT, realmName), -1))
            .setMaxCachedSessions(Integer.getInteger(String.format(BRUTE_FORCE_MAX_CACHED_SESSIONS, realmName), -1));
      if (realm instanceof ModifiableSecurityRealm) {
         return wrapper.wrap(ModifiableSecurityRealm.class);
      } else {
         return wrapper.wrap(SecurityRealm.class);
      }
   }

   private static ScheduledExecutorService getBruteForceExecutor() {
      if (bruteForceExecutor == null) {
         synchronized (RealmConfiguration.class) {
            if (bruteForceExecutor == null) {
               bruteForceExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                  Thread t = new Thread(r, "brute-force-protection");
                  t.setDaemon(true);
                  return t;
               });
            }
         }
      }
      return bruteForceExecutor;
   }

   private SecurityRealm cacheable(SecurityRealm realm) {
      int maxEntries = attributes.attribute(CACHE_MAX_SIZE).get();
      if (maxEntries > 0 && realm instanceof CacheableSecurityRealm) {
         if (cache == null) {
            cache = new LRURealmIdentityCache(maxEntries, attributes.attribute(CACHE_LIFESPAN).get().longValue());
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

   public boolean hasFeature(ServerSecurityRealm.Feature feature) {
      return features.contains(feature);
   }

   void setHttpChallengeReadiness(Supplier<Boolean> httpChallengeReadiness) {
      this.httpChallengeReadiness = httpChallengeReadiness;
   }

   public void flushCache() {
      if (cache != null) {
         cache.clear();
      }
   }
}
