package org.infinispan.rest.resources;

import static org.infinispan.client.rest.configuration.Protocol.HTTP_11;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;
import static org.infinispan.rest.RequestHeader.KEY_CONTENT_TYPE_HEADER;
import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.apache.logging.log4j.core.util.StringBuilderWriter;
import org.infinispan.client.rest.RestClient;
import org.infinispan.client.rest.RestEntity;
import org.infinispan.client.rest.RestResponse;
import org.infinispan.client.rest.configuration.Protocol;
import org.infinispan.client.rest.configuration.RestClientConfigurationBuilder;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.io.ConfigurationWriter;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.test.security.TestCertificates;
import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.CounterConfigurationSerializer;
import org.infinispan.factories.impl.BasicComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestTestSCI;
import org.infinispan.rest.TestClass;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.authentication.impl.BasicAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.rest.resources.security.SimpleSecurityDomain;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.security.mappers.IdentityRoleMapper;
import org.infinispan.server.core.DummyServerStateManager;
import org.infinispan.server.core.ServerStateManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.netty.buffer.ByteBufUtil;
import io.netty.util.ResourceLeakDetector;

@Test(groups = "functional")
public class AbstractRestResourceTest extends MultipleCacheManagersTest {
   public static final String REALM = "ApplicationRealm";
   public static final Subject ADMIN = TestingUtil.makeSubject("ADMIN");
   public static final Subject USER = TestingUtil.makeSubject("USER");

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();
   protected RestClient client;
   protected RestClient adminClient;
   protected static final int NUM_SERVERS = 2;
   private final List<RestServerHelper> restServers = new ArrayList<>(NUM_SERVERS);

   protected boolean security;
   protected Protocol protocol = HTTP_11;
   protected boolean ssl;
   protected boolean browser;

   protected ServerStateManager serverStateManager;

   @Override
   protected String parameters() {
      return "[security=" + security + ", protocol=" + protocol.toString() + ", ssl=" + ssl + ", browser=" + browser + "]";
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

   protected AbstractRestResourceTest browser(boolean browser) {
      this.browser = browser;
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
      globalBuilder.serialization().addContextInitializers(RestTestSCI.INSTANCE);
      if (isSecurityEnabled()) addSecurity(globalBuilder);
      return globalBuilder.clusteredDefault().cacheManagerName("default");
   }

   protected void addSecurity(GlobalConfigurationBuilder globalBuilder) {
      globalBuilder.security().authorization().enable().groupOnlyMapping(false).principalRoleMapper(new IdentityRoleMapper())
            .role("ADMIN").description("admin role").permission(AuthorizationPermission.ALL)
            .role("USER").description("user role").permission(AuthorizationPermission.WRITE, AuthorizationPermission.READ, AuthorizationPermission.EXEC, AuthorizationPermission.BULK_READ, AuthorizationPermission.CREATE);
   }

   @Override
   protected void createCacheManagers() throws Exception {
      Security.doAs(ADMIN, () -> {
         for (int i = 0; i < NUM_SERVERS; i++) {
            GlobalConfigurationBuilder configForNode = getGlobalConfigForNode(i);
            addClusterEnabledCacheManager(new GlobalConfigurationBuilder().read(configForNode.build()), getDefaultCacheBuilder(), TransportFlags.minimalXsiteFlags());
         }
         cacheManagers.forEach(this::defineCaches);
         serverStateManager = new DummyServerStateManager();
         for (EmbeddedCacheManager cm : cacheManagers) {
            BasicComponentRegistry bcr = SecurityActions.getGlobalComponentRegistry(cm).getComponent(BasicComponentRegistry.class);
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
            configureServer(restServerHelper);
            restServerHelper.start(TestResourceTracker.getCurrentTestShortName());
            restServers.add(restServerHelper);
         }
      });
   }

   protected RestServerHelper configureServer(RestServerHelper helper) {
      if (isSecurityEnabled()) {
         BasicAuthenticator basicAuthenticator = new BasicAuthenticator(new SimpleSecurityDomain(ADMIN, USER), REALM);
         helper.withAuthenticator(basicAuthenticator);
      }
      if (ssl) {
         helper.withKeyStore(TestCertificates.certificate("server"), TestCertificates.KEY_PASSWORD, TestCertificates.KEYSTORE_TYPE)
               .withTrustStore(TestCertificates.certificate("trust"), TestCertificates.KEY_PASSWORD, TestCertificates.KEYSTORE_TYPE);
      }
      return helper;
   }

   protected RestServerHelper restServer() {
      return restServers.get(0);
   }

   protected void defineCaches(EmbeddedCacheManager cm) {
   }

   @BeforeClass(alwaysRun = true)
   public void beforeClass() {
      ByteBufUtil.setLeakListener(new ResourceLeakDetector.LeakListener() {
         @Override
         public void onLeak(String resourceType, String records) {
            throw new RuntimeException(resourceType + ": " + records);
         }
      });
   }

   @AfterClass
   public void afterClass() {
      Security.doAs(ADMIN, () -> restServers.forEach(RestServerHelper::stop));
      restServers.clear();
   }

   @BeforeMethod
   public void beforeMethod() {
      adminClient = RestClient.forConfiguration(getClientConfig("admin", "admin").build());
      client = RestClient.forConfiguration(getClientConfig("user", "user").build());
   }

   @AfterMethod
   public void afterMethod() {
      Util.close(client);
      Util.close(adminClient);
      Security.doAs(ADMIN, () -> restServers.forEach(RestServerHelper::clear));
   }

   private void putInCache(String cacheName, Object key, String keyContentType, String value, String contentType) {
      String url = String.format("/rest/v2/caches/%s/%s", cacheName, key);
      Map<String, String> headers = new HashMap<>();
      if (keyContentType != null) headers.put(KEY_CONTENT_TYPE_HEADER.toString(), contentType);

      CompletionStage<RestResponse> response = client.raw().put(url, headers, RestEntity.create(MediaType.fromString(contentType), value));

      ResponseAssertion.assertThat(response).isOk();
   }

   void putInCache(String cacheName, Object key, String value, String contentType) {
      putInCache(cacheName, key, null, value, contentType);
   }

   void putStringValueInCache(String cacheName, String key, String value) {
      putInCache(cacheName, key, value, "text/plain; charset=utf-8");
   }

   void putTextEntryInCache(String cacheName, String key, String value) {
      putInCache(cacheName, key, TEXT_PLAIN_TYPE, value, TEXT_PLAIN_TYPE);
   }

   void putJsonValueInCache(String cacheName, String key, String value) {
      putInCache(cacheName, key, value, "application/json; charset=utf-8");
   }

   void putBinaryValueInCache(String cacheName, String key, byte[] value, MediaType mediaType) {
      RestEntity restEntity = RestEntity.create(mediaType, value);
      CompletionStage<RestResponse> response = client.cache(cacheName).put(key, restEntity);
      ResponseAssertion.assertThat(response).isOk();
   }

   private void removeFromCache(String cacheName, Object key, String keyContentType) {
      String url = String.format("/rest/v2/caches/%s/%s", cacheName, key);
      Map<String, String> headers = new HashMap<>();
      if (keyContentType != null) headers.put(KEY_CONTENT_TYPE_HEADER.toString(), keyContentType);

      CompletionStage<RestResponse> response = client.raw().delete(url, headers);

      ResponseAssertion.assertThat(response).isOk();
   }

   void removeTextEntryFromCache(String cacheName, String key) {
      removeFromCache(cacheName, key, TEXT_PLAIN_TYPE);
   }

   protected RestClientConfigurationBuilder getClientConfig(String username, String password) {
      RestClientConfigurationBuilder clientConfigurationBuilder = new RestClientConfigurationBuilder();
      if (protocol != null) {
         clientConfigurationBuilder.protocol(protocol);
      }
      if (ssl) {
         clientConfigurationBuilder.security().ssl().enable()
               .hostnameVerifier((hostname, session) -> true)
               .trustStoreFileName(TestCertificates.certificate("ca")).trustStorePassword(TestCertificates.KEY_PASSWORD).trustStoreType(TestCertificates.KEYSTORE_TYPE)
               .keyStoreFileName(TestCertificates.certificate("client")).keyStorePassword(TestCertificates.KEY_PASSWORD).keyStoreType(TestCertificates.KEYSTORE_TYPE);
      }
      if (isSecurityEnabled()) {
         clientConfigurationBuilder.security().authentication().enable().username(username).password(password);
      }
      if (browser) {
         clientConfigurationBuilder.header("User-Agent", "Mozilla/5.0 (X11; Fedora; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/112.0");
      }
      restServers.forEach(s -> clientConfigurationBuilder.addServer().host(s.getHost()).port(s.getPort()));
      return clientConfigurationBuilder;
   }

   public static String cacheConfigToJson(String name, Configuration configuration) {
      StringBuilderWriter sw = new StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).prettyPrint(false).build()) {
         new ParserRegistry().serialize(w, name, configuration);
      }
      return sw.toString();
   }

   public static String counterConfigToJson(AbstractCounterConfiguration config) {
      org.infinispan.commons.io.StringBuilderWriter sw = new org.infinispan.commons.io.StringBuilderWriter();
      try (ConfigurationWriter w = ConfigurationWriter.to(sw).withType(APPLICATION_JSON).build()) {
         new CounterConfigurationSerializer().serializeConfiguration(w, config);
      }
      return sw.toString();
   }


   protected RestResponse join(CompletionStage<RestResponse> responseStage) {
      RestResponse response = CompletionStages.join(responseStage);
      checkBrowserHeaders(response);
      return response;
   }

   protected void checkBrowserHeaders(RestResponse response) {
      if (browser) {
         assertEquals("sameorigin", response.header("X-Frame-Options"));
         assertEquals("1; mode=block", response.header("X-XSS-Protection"));
         assertEquals("nosniff", response.header("X-Content-Type-Options"));
         if (ssl) {
            assertEquals("max-age=31536000 ; includeSubDomains", response.header("Strict-Transport-Security"));
         }
      }
   }
}
