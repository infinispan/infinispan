package org.infinispan.rest.resources;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.security.auth.Subject;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.remote.ProtobufMetadataManager;
import org.infinispan.rest.RestTestSCI;
import org.infinispan.rest.TestClass;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.authentication.impl.BasicAuthenticator;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.rest.resources.security.AuthClient;
import org.infinispan.rest.resources.security.SimpleSecurityDomain;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.Security;
import org.infinispan.security.mappers.IdentityRoleMapper;
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
   public static final Subject USER = TestingUtil.makeSubject("USER", ProtobufMetadataManager.SCHEMA_MANAGER_ROLE, ScriptingManager.SCRIPT_MANAGER_ROLE);

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();
   protected HttpClient client;
   private static final int NUM_SERVERS = 2;
   private List<RestServerHelper> restServers = new ArrayList<>(NUM_SERVERS);

   protected boolean security;

   @Override
   protected String parameters() {
      return "[security=" + security + "]";
   }

   protected AbstractRestResourceTest withSecurity(boolean security) {
      this.security = security;
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
                   .role("USER").permission(AuthorizationPermission.ALL);
   }

   @Override
   protected void createCacheManagers() throws Exception {
      Security.doAs(ADMIN_USER, (PrivilegedAction<Void>) () -> {
         for (int i = 0; i < NUM_SERVERS; i++) {
            GlobalConfigurationBuilder configForNode = getGlobalConfigForNode(i);
            addClusterEnabledCacheManager(new GlobalConfigurationBuilder().read(configForNode.build()), getDefaultCacheBuilder(), TransportFlags.minimalXsiteFlags());
         }
         cacheManagers.forEach(this::defineCaches);
         for (EmbeddedCacheManager cm : cacheManagers) {
            Set<String> cacheNames = cm.getCacheNames();
            cacheNames.forEach(cm::getCache);
            cm.getClassWhiteList().addClasses(TestClass.class);
            waitForClusterToForm(cacheNames.toArray(new String[0]));
            RestServerHelper restServerHelper = new RestServerHelper(cm);
            if (isSecurityEnabled()) {
               BasicAuthenticator basicAuthenticator = new BasicAuthenticator(new SimpleSecurityDomain(USER), REALM);
               restServerHelper.withAuthenticator(basicAuthenticator);
            }
            restServerHelper.start(TestResourceTracker.getCurrentTestShortName() + "-" + cm.getAddress());
            restServers.add(restServerHelper);
         }
         return null;
      });
      client = createNewClient();
      client.start();
   }

   protected RestServerHelper restServer() {
      return restServers.get(0);
   }

   protected void defineCaches(EmbeddedCacheManager cm) {
   }

   @AfterClass
   public void afterSuite() {
      Subject.doAs(ADMIN_USER, (PrivilegedAction<Void>) () -> {
         try {
            client.stop();
         } catch (Exception ignored) {
         }
         restServers.forEach(RestServerHelper::stop);
         return null;
      });
   }

   @AfterMethod
   public void afterMethod() {
      Subject.doAs(ADMIN_USER, (PrivilegedAction<Void>) () -> {
         restServers.forEach(RestServerHelper::clear);
         return null;
      });
   }

   private void putInCache(String cacheName, Object key, String keyContentType, String value, String contentType) throws InterruptedException, ExecutionException, TimeoutException {
      Request request = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), cacheName, key))
            .content(new StringContentProvider(value))
            .header("Content-type", contentType)
            .method(HttpMethod.PUT);
      if (keyContentType != null) request.header("Key-Content-type", keyContentType);

      ContentResponse response = request.send();
      ResponseAssertion.assertThat(response).isOk();
   }

   void putInCache(String cacheName, Object key, String value, String contentType) throws InterruptedException, ExecutionException, TimeoutException {
      putInCache(cacheName, key, null, value, contentType);
   }

   void putStringValueInCache(String cacheName, String key, String value) throws InterruptedException, ExecutionException, TimeoutException {
      putInCache(cacheName, key, value, "text/plain; charset=utf-8");
   }

   void putJsonValueInCache(String cacheName, String key, String value) throws InterruptedException, ExecutionException, TimeoutException {
      putInCache(cacheName, key, value, "application/json; charset=utf-8");
   }

   void putBinaryValueInCache(String cacheName, String key, byte[] value, MediaType mediaType) throws InterruptedException, ExecutionException, TimeoutException {
      ContentResponse response = client
            .newRequest(String.format("http://localhost:%d/rest/v2/caches/%s/%s", restServer().getPort(), cacheName, key))
            .content(new BytesContentProvider(value))
            .header(HttpHeader.CONTENT_TYPE, mediaType.toString())
            .method(HttpMethod.PUT)
            .send();
      ResponseAssertion.assertThat(response).isOk();
   }

   protected HttpClient createNewClient() {
      if (isSecurityEnabled()) {
         return new AuthClient("user", "user");
      }
      return new HttpClient();
   }
}
