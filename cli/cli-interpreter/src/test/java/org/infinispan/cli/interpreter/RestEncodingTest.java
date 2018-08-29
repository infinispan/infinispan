package org.infinispan.cli.interpreter;

import static org.infinispan.server.core.test.ServerTestingUtil.findFreePort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;


@Test(testName = "cli.interpreter.RestEncodingTest", groups = "functional")
public class RestEncodingTest extends SingleCacheManagerTest {

   private static final String REGULAR_CACHE = "default";
   private static final String OBJ_CACHE = "object";

   private CloseableHttpClient restClient = HttpClients.createMinimal();
   private RestServer restServer;
   private Interpreter interpreter;


   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder c = hotRodCacheConfiguration(getDefaultStandaloneCacheConfig(false));
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(c);

      ConfigurationBuilder cfgBuilder = new ConfigurationBuilder();
      cfgBuilder.encoding().key().mediaType(MediaType.APPLICATION_OBJECT_TYPE)
            .encoding().value().mediaType(MediaType.APPLICATION_OBJECT_TYPE);

      ConfigurationBuilder defaultBuilder = new ConfigurationBuilder();

      cacheManager.defineConfiguration(OBJ_CACHE, cfgBuilder.build());
      cacheManager.defineConfiguration(REGULAR_CACHE, defaultBuilder.build());

      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder().port(findFreePort());

      restServer = new RestServer();
      restServer.start(builder.build(), cacheManager);

      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cacheManager);
      interpreter = gcr.getComponent(Interpreter.class);

      return cacheManager;
   }

   public void testRestCodec() throws Exception {
      testRestCodecWithCache(REGULAR_CACHE);
   }

   public void testRestCodecWithObjects() throws Exception {
      testRestCodecWithCache(OBJ_CACHE);
   }

   public void testRestEncoding() throws Exception {
      testRestEncodingWithCache(REGULAR_CACHE);
   }

   public void testRestEncodingWithObjects() throws Exception {
      testRestEncodingWithCache(OBJ_CACHE);
   }

   private String getRestEndpoint(String cache, String key) {
      return String.format("http://localhost:%s/rest/%s/%s", restServer.getPort(), cache, key);
   }

   private void testRestCodecWithCache(String cacheName) throws Exception {
      writeViaRest(cacheName, "k1", "v1");

      String sessionId = interpreter.createSessionId(cacheName);
      Map<String, String> response = interpreter.execute(sessionId, "get --codec=rest k1;");
      assertEquals("v1", response.get(ResultKeys.OUTPUT.toString()));

      interpreter.execute(sessionId, "remove --codec=rest k1;");
      assertNull(readViaRest(cacheName, "k1"));

      interpreter.execute(sessionId, "put --codec=rest k2 v2;");
      readViaRest(cacheName, "k2");
      assertEquals("v2", readViaRest(cacheName, "k2"));

      interpreter.execute(sessionId, "evict --codec=rest k2;");
      assertNull(readViaRest(cacheName, "k2"));
   }

   private void testRestEncodingWithCache(String cacheName) throws Exception {
      writeViaRest(cacheName, "key", "value");

      String sessionId = interpreter.createSessionId(cacheName);
      interpreter.execute(sessionId, "encoding rest;");

      assertEquals("value", readViaRest(cacheName, "key"));
      assertEquals("value", readViaCLI(sessionId, "key"));

      writeViaCLI(sessionId, "key2", "value2");
      assertEquals("value2", readViaRest(cacheName, "key2"));
      assertEquals("value2", readViaCLI(sessionId, "key2"));
   }

   private String readViaRest(String cache, String key) throws Exception {
      HttpGet httpGet = new HttpGet(getRestEndpoint(cache, key));
      httpGet.addHeader("Accept", MediaType.TEXT_PLAIN_TYPE);
      CloseableHttpResponse response = restClient.execute(httpGet);
      int statusCode = response.getStatusLine().getStatusCode();
      if(statusCode == HttpStatus.SC_NOT_FOUND) {
         return null;
      }
      assertEquals(HttpStatus.SC_OK, statusCode);
      HttpEntity entity = response.getEntity();
      return EntityUtils.toString(entity);
   }

   private void writeViaRest(String cache, String key, String value) throws Exception {
      HttpPost httpPost = new HttpPost(getRestEndpoint(cache, key));
      httpPost.setEntity(new StringEntity(value));
      CloseableHttpResponse response = restClient.execute(httpPost);
      assertEquals(HttpStatus.SC_OK, response.getStatusLine().getStatusCode());
      EntityUtils.consume(response.getEntity());
   }

   private String readViaCLI(String sessionId, String key) throws Exception {
      Map<String, String> response = interpreter.execute(sessionId, String.format("get %s;", key));
      assertNull(response.get(ResultKeys.ERROR.toString()));
      return response.get(ResultKeys.OUTPUT.toString());
   }

   private void writeViaCLI(String sessionId, String key, String value) throws Exception {
      Map<String, String> response = interpreter.execute(sessionId, String.format("put %s %s;", key, value));
      assertNull(response.get(ResultKeys.ERROR.toString()));
   }

   @AfterClass
   protected void teardown() {
      try {
         restClient.close();
         if (restServer != null) {
            restServer.stop();
         }
         killCacheManagers(cacheManager);
      } catch (Exception ignored) {
      }
   }

}
