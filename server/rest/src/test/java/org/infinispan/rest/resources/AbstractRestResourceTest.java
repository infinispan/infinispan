package org.infinispan.rest.resources;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestTestSCI;
import org.infinispan.rest.TestClass;
import org.infinispan.rest.assertion.ResponseAssertion;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestResourceTracker;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional")
public abstract class AbstractRestResourceTest extends MultipleCacheManagersTest {
   protected HttpClient client;
   private static final int NUM_SERVERS = 2;
   private List<RestServerHelper> restServers = new ArrayList<>(NUM_SERVERS);

   public ConfigurationBuilder getDefaultCacheBuilder() {
      return getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
   }

   protected GlobalConfigurationBuilder getGlobalConfigForNode(int id) {
      GlobalConfigurationBuilder globalBuilder = new GlobalConfigurationBuilder();
      globalBuilder.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      globalBuilder.globalJmxStatistics().enable();
      globalBuilder.serialization().addContextInitializer(RestTestSCI.INSTANCE);
      return globalBuilder.clusteredDefault().cacheManagerName("default");
   }

   @Override
   protected void createCacheManagers() throws Exception {
      for (int i = 0; i < NUM_SERVERS; i++) {
         GlobalConfigurationBuilder configForNode = getGlobalConfigForNode(i);
         addClusterEnabledCacheManager(new GlobalConfigurationBuilder().read(configForNode.build()), getDefaultCacheBuilder());
      }
      cacheManagers.forEach(this::defineCaches);
      for (EmbeddedCacheManager cm : cacheManagers) {
         String[] cacheNames = cm.getCacheNames().toArray(new String[0]);
         cm.startCaches(cacheNames);
         cm.getClassWhiteList().addClasses(TestClass.class);
         waitForClusterToForm(cacheNames);
         RestServerHelper restServerHelper = new RestServerHelper(cm);
         restServerHelper.start(TestResourceTracker.getCurrentTestShortName());
         restServers.add(restServerHelper);
      }
      client = new HttpClient();
      client.start();
   }

   protected RestServerHelper restServer() {
      return restServers.get(0);
   }

   abstract protected void defineCaches(EmbeddedCacheManager cm);

   @AfterClass
   public void afterSuite() throws Exception {
      client.stop();
      restServers.forEach(RestServerHelper::stop);
   }

   @AfterMethod
   public void afterMethod() {
      restServers.forEach(RestServerHelper::clear);
   }


   private void putInCache(String cacheName, Object key, String keyContentType, String value, String contentType) throws InterruptedException, ExecutionException, TimeoutException {
      Request request = client
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer().getPort(), cacheName, key))
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
            .newRequest(String.format("http://localhost:%d/rest/%s/%s", restServer().getPort(), cacheName, key))
            .content(new BytesContentProvider(value))
            .header(HttpHeader.CONTENT_TYPE, mediaType.toString())
            .method(HttpMethod.PUT)
            .send();
      ResponseAssertion.assertThat(response).isOk();
   }


}
