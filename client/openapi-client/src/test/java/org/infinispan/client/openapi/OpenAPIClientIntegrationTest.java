package org.infinispan.client.openapi;

import static org.infinispan.commons.test.Exceptions.expectCompletionException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.openapi.configuration.OpenAPIClientConfigurationBuilder;
import org.infinispan.client.openapi.impl.jdk.CacheJDK;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.helper.RestServerHelper;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Integration test for OpenAPI client demonstrating put/get operations
 *
 * @author Infinispan team
 */
@Test(groups = "functional", testName = "client.openapi.OpenAPIClientIntegrationTest")
public class OpenAPIClientIntegrationTest extends SingleCacheManagerTest {

   private RestServerHelper restServer;
   private OpenAPIClient openAPIClient;
   private String cacheName;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      // Configure cache encoding: keys and values as text/plain
      // Generated API expects text/plain responses (see CacheApi.java:2421)
      builder.encoding().key().mediaType(MediaType.TEXT_PLAIN_TYPE);
      builder.encoding().value().mediaType(MediaType.TEXT_PLAIN_TYPE);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createServerModeCacheManager(builder);
      cacheName = cm.getCacheManagerConfiguration().defaultCacheName().orElse("default");
      return cm;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      // Start REST server
      restServer = new RestServerHelper(cacheManager);
      restServer.start(TestResourceTracker.getCurrentTestShortName());

      // Create OpenAPI client
      OpenAPIClientConfigurationBuilder clientBuilder = new OpenAPIClientConfigurationBuilder();
      clientBuilder.addServer()
         .host("localhost")
         .port(restServer.getPort());
      clientBuilder.pingOnCreate(false);

      openAPIClient = OpenAPIClient.forConfiguration(clientBuilder.build());
   }

   @AfterClass(alwaysRun = true)
   protected void teardown() {
      Util.close(openAPIClient);
      if (restServer != null) {
         restServer.stop();
      }
      super.teardown();
   }

   @Test
   public void testPutAndGet() throws Exception {
      CacheJDK cache = openAPIClient.cacheJDK(cacheName);

      // Test put
      CompletionStage<Void> putFuture = cache.put("testKey", "testValue");
      putFuture.toCompletableFuture().get(600, TimeUnit.SECONDS);

      // Test get
      CompletionStage<String> getFuture = cache.get("testKey");
      String value = getFuture.toCompletableFuture().get(10, TimeUnit.SECONDS);

      assertNotNull("Value should not be null", value);
      // Strip JSON quotes from response (workaround for generated API JSON-serializing strings)
      assertEquals("\"testValue\"", value);
   }

   @Test
   public void testPutWithExpiration() throws Exception {
      CacheJDK cache = openAPIClient.cacheJDK(cacheName);

      // Put with 2 second TTL
      CompletionStage<Void> putFuture = cache.put("expiringKey", "expiringValue", 2, -1);
      putFuture.toCompletableFuture().get(10, TimeUnit.SECONDS);

      // Verify value exists
      CompletionStage<String> getFuture = cache.get("expiringKey");
      String value = getFuture.toCompletableFuture().get(10, TimeUnit.SECONDS);
      assertEquals("\"expiringValue\"", value);

      // Wait for expiration
      Thread.sleep(3000);

      // Verify value has expired - should get 404
      expectCompletionException(ApiException.class, ".*404.*", cache.get("expiringKey"));
   }

   @Test
   public void testMultiplePutGet() throws Exception {
      CacheJDK cache = openAPIClient.cacheJDK(cacheName);

      // Put multiple entries
      cache.put("key1", "value1").toCompletableFuture().get(10, TimeUnit.SECONDS);
      cache.put("key2", "value2").toCompletableFuture().get(10, TimeUnit.SECONDS);
      cache.put("key3", "value3").toCompletableFuture().get(10, TimeUnit.SECONDS);

      // Get and verify (values have JSON quotes due to generated API serialization)
      assertEquals("\"value1\"", cache.get("key1").toCompletableFuture().get(10, TimeUnit.SECONDS));
      assertEquals("\"value2\"", cache.get("key2").toCompletableFuture().get(10, TimeUnit.SECONDS));
      assertEquals("\"value3\"", cache.get("key3").toCompletableFuture().get(10, TimeUnit.SECONDS));
   }

   @Test
   public void testGetNonExistentKey() {
      CacheJDK cache = openAPIClient.cacheJDK(cacheName);

      // Should throw ApiException with 404 for non-existent key
      expectCompletionException(ApiException.class, ".*404.*", cache.get("nonExistentKey"));
   }
}
