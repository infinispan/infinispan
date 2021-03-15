package org.infinispan.rest.resources;

import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_OBJECT_TYPE;
import static org.infinispan.rest.RequestHeader.KEY_CONTENT_TYPE_HEADER;
import static org.infinispan.rest.helper.RestServerHelper.CLIENT_KEY_STORE;
import static org.infinispan.rest.helper.RestServerHelper.SERVER_KEY_STORE;
import static org.infinispan.rest.helper.RestServerHelper.STORE_PASSWORD;
import static org.infinispan.rest.helper.RestServerHelper.STORE_TYPE;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.rest.RestTestSCI;
import org.infinispan.rest.TestClass;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.authentication.impl.BasicAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.rest.resources.security.SimpleSecurityDomain;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.server.core.DummyServerStateManager;
import org.infinispan.server.core.ServerStateManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional")
public class AbstractRestResourceTest extends MultipleCacheManagersTest {
   public static final String REALM = "ApplicationRealm";
   public static final Subject ADMIN_USER = TestingUtil.makeSubject("ADMIN", ScriptingManager.SCRIPT_MANAGER_ROLE, ProtobufMetadataManager.SCHEMA_MANAGER_ROLE);
   public static final Subject USER = TestingUtil.makeSubject("USER", ScriptingManager.SCRIPT_MANAGER_ROLE, ProtobufMetadataManager.SCHEMA_MANAGER_ROLE);

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();
   protected RestClient client;
   private static final int NUM_SERVERS = 2;
   private final List<RestServerHelper> restServers = new ArrayList<>(NUM_SERVERS);

   protected boolean security;
   protected Protocol protocol = HTTP_11;
   protected boolean ssl;

   protected ServerStateManager serverStateManager;

   @Override
   protected String parameters() {
      return "[security=" + security + ", protocol=" + protocol.toString() + ", ssl=" + ssl + "]";
   }

   protected AbstractRestResourceTest withSecurity(boolean security) {
      this.security = security;
      return this;
   }

   protected AbstractRestResourceTest protocol(Protocol protocol) {
      this.protocol = protocol;
      return this;
   }

   protected AbstractRestResourceTest ssl(boolean ssl) {
      this.ssl = ssl;
      return this;
   }

   public ConfigurationBuilder getDefaultCacheBuilder() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   protected boolean isSecurityEnabled() {
      return security;
   }

   protected GlobalConfigurationBuilder getGlobalConfigForNode(int id) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      TestCacheManagerFactory.configureJmx(globalBuilder, getClass().getSimpleName() + id, mBeanServerLookup);
      globalBuilder.cacheContainer().statistics(true);
      globalBuilder.serialization().addContextInitializer(RestTestSCI.INSTANCE);
      if (isSecurityEnabled()) addSecurity(globalBuilder);
      return globalBuilder.clusteredDefault().cacheManagerName("default");
   }

   protected void addSecurity(GlobalConfigurationBuilder globalBuilder) {
      globalBuilder.security().authorization().enable().principalRoleMapper(new IdentityRoleMapper())
            .role("ADMIN").permission(AuthorizationPermission.ALL)
            .role("USER").permission(AuthorizationPermission.WRITE, AuthorizationPermission.READ, AuthorizationPermission.EXEC);
   }

   @Override
   protected void createCacheManagers() throws Exception {
      Security.doAs(ADMIN_USER, () -> {
         for (int i = 0; i < NUM_SERVERS; i++) {
            GlobalConfigurationBuilder configForNode = getGlobalConfigForNode(i);
            addClusterEnabledCacheManager(new GlobalConfigurationBuilder().read(configForNode.build()), getDefaultCacheBuilder(), TransportFlags.minimalXsiteFlags());
         }
         cacheManagers.forEach(this::defineCaches);
         cacheManagers.forEach(cm -> cm.defineConfiguration("invalid", getDefaultCacheBuilder().encoding().mediaType(APPLICATION_OBJECT_TYPE).indexing().enabled(true).addIndexedEntities("invalid").build()));
         serverStateManager = new DummyServerStateManager();
         for (EmbeddedCacheManager cm : cacheManagers) {
            BasicComponentRegistry bcr = SecurityActions.getGlobalComponentRegistry(cm).getComponent(BasicComponentRegistry.class.getName());
            bcr.registerComponent(ServerStateManager.class, serverStateManager, false);
            cm.getClassAllowList().addClasses(TestClass.class);
            waitForClusterToForm(cm.getCacheNames().stream().filter(name -> {
               try {
                  cm.getCache(name);
                  return true;
               } catch (CacheConfigurationException ignored) {
                  return false;
               }
            }).toArray(String[]::new));
            RestServerHelper restServerHelper = new RestServerHelper(cm);
            if (isSecurityEnabled()) {
               BasicAuthenticator basicAuthenticator = new BasicAuthenticator(new SimpleSecurityDomain(ADMIN_USER, USER), REALM);
               restServerHelper.withAuthenticator(basicAuthenticator);
            }
            if (ssl) {
               restServerHelper.withKeyStore(SERVER_KEY_STORE, STORE_PASSWORD, STORE_TYPE)
                     .withTrustStore(SERVER_KEY_STORE, STORE_PASSWORD, STORE_TYPE);
            }
            restServerHelper.start(TestResourceTracker.getCurrentTestShortName());
            restServers.add(restServerHelper);
         }
      });
      client = RestClient.forConfiguration(getClientConfig().build());
   }

   protected RestServerHelper restServer() {
      return restServers.get(0);
   }

   protected void defineCaches(EmbeddedCacheManager cm) {
   }

   @AfterClass
   public void afterClass() {
      Security.doAs(ADMIN_USER, (PrivilegedAction<Void>) () -> {
         restServers.forEach(RestServerHelper::stop);
         return null;
      });
      Util.close(client);
   }

   @AfterMethod
   public void afterMethod() {
      Security.doAs(ADMIN_USER, (PrivilegedAction<Void>) () -> {
         restServers.forEach(RestServerHelper::clear);
         return null;
      });
   }

   private void putInCache(String cacheName, Object key, String keyContentType, String value, String contentType) {
      String url = String.format("/rest/v2/caches/%s/%s", cacheName, key);
      Map<String, String> headers = new HashMap<>();
      if (keyContentType != null) headers.put(KEY_CONTENT_TYPE_HEADER.getValue(), contentType);

      CompletionStage<RestResponse> response = client.raw().putValue(url, headers, value, contentType);

      ResponseAssertion.assertThat(response).isOk();
   }

   void putInCache(String cacheName, Object key, String value, String contentType) {
      putInCache(cacheName, key, null, value, contentType);
   }

   void putStringValueInCache(String cacheName, String key, String value) {
      putInCache(cacheName, key, value, "text/plain; charset=utf-8");
   }

   void putJsonValueInCache(String cacheName, String key, String value) {
      putInCache(cacheName, key, value, "application/json; charset=utf-8");
   }

   void putBinaryValueInCache(String cacheName, String key, byte[] value, MediaType mediaType) {
      RestEntity restEntity = RestEntity.create(mediaType, value);
      CompletionStage<RestResponse> response = client.cache(cacheName).put(key, restEntity);
      ResponseAssertion.assertThat(response).isOk();
   }

   protected RestClientConfigurationBuilder getClientConfig() {
      RestClientConfigurationBuilder clientConfigurationBuilder = new RestClientConfigurationBuilder();
      if (protocol != null) {
         clientConfigurationBuilder.protocol(protocol);
      }
      if (ssl) {
         clientConfigurationBuilder.security().ssl().enable()
               .hostnameVerifier((hostname, session) -> true)
               .trustStoreFileName(CLIENT_KEY_STORE).trustStorePassword(STORE_PASSWORD).trustStoreType(STORE_TYPE)
               .keyStoreFileName(CLIENT_KEY_STORE).keyStorePassword(STORE_PASSWORD).keyStoreType(STORE_TYPE);
      }
      if (isSecurityEnabled()) {
         clientConfigurationBuilder.security().authentication().enable().username("user").password("user");
      }
      restServers.forEach(s -> clientConfigurationBuilder.addServer().host(s.getHost()).port(s.getPort()));
      return clientConfigurationBuilder;
   }
}
