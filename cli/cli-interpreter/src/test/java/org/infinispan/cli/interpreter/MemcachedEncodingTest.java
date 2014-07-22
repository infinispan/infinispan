package org.infinispan.cli.interpreter;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import net.spy.memcached.MemcachedClient;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.CacheValue;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.memcached.MemcachedServer;
import org.infinispan.server.memcached.test.MemcachedTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;
/**
 * MemcachedEncodingTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(testName = "cli-server.MemcachedEncodingTest", groups = "unstable", description = "original group: functional")
@CleanupAfterMethod
public class MemcachedEncodingTest extends SingleCacheManagerTest {

   private static final String MEMCACHED_CACHE = "memcachedCache";
   MemcachedServer memcachedServer;
   int port;
   Interpreter interpreter;
   MemcachedClient memcachedClient;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(false);
      c.jmxStatistics().enable();
      cacheManager = TestCacheManagerFactory.createCacheManager(c);
      memcachedServer = MemcachedTestingUtil.startMemcachedTextServer(cacheManager);
      port = memcachedServer.getPort();
      memcachedClient = MemcachedTestingUtil.createMemcachedClient(60000, port);
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cacheManager);
      interpreter = gcr.getComponent(Interpreter.class);
      return cacheManager;
   }

   @AfterMethod
   public void release() {
      MemcachedTestingUtil.killMemcachedServer(memcachedServer);
      TestingUtil.killCacheManagers(cacheManager);
      MemcachedTestingUtil.killMemcachedClient(memcachedClient);
   }
   /* TODO: convert to new encoding
   public void testMemcachedCodec() throws Exception {
      Cache<ByteArrayKey, CacheValue> cache = cacheManager.getCache(MEMCACHED_CACHE);

      memcachedClient.set("k1", 3600, "v1").get();

      assertTrue(cache.containsKey("k1"));

      String sessionId = interpreter.createSessionId(MEMCACHED_CACHE);
      Map<String, String> response = interpreter.execute(sessionId, "get --codec=memcached k1;");
      assertEquals("v1", response.get(ResultKeys.OUTPUT.toString()));

      interpreter.execute(sessionId, "put --codec=memcached k2 v2;");
      String v2 = (String) memcachedClient.get("k2");
      assertEquals("v2", v2);
   }

   public void testMemcachedEncoding() throws Exception {
      Cache<ByteArrayKey, CacheValue> cache = cacheManager.getCache(MEMCACHED_CACHE);

      memcachedClient.set("k1", 3600, "v1").get();

      assertTrue(cache.containsKey("k1"));

      String sessionId = interpreter.createSessionId(MEMCACHED_CACHE);
      interpreter.execute(sessionId, "encoding memcached;");
      Map<String, String> response = interpreter.execute(sessionId, "get k1;");
      assertEquals("v1", response.get(ResultKeys.OUTPUT.toString()));

      interpreter.execute(sessionId, "put k2 v2;");
      String v2 = (String) memcachedClient.get("k2");
      assertEquals("v2", v2);
   }
   */
}
