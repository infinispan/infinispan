package org.infinispan.cli.interpreter;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.cli.interpreter.result.ResultKeys;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

/**
 * EncodingTest.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@Test(testName = "cli.interpreter.HotRodEncodingTest", groups = "functional")
@CleanupAfterMethod
public class HotRodEncodingTest extends SingleCacheManagerTest {

   HotRodServer hotrodServer;
   int port;
   Interpreter interpreter;
   private RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = hotRodCacheConfiguration(
            getDefaultStandaloneCacheConfig(false));
      c.jmxStatistics().enable();
      return TestCacheManagerFactory.createCacheManager(c);
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      port = hotrodServer.getPort();
      remoteCacheManager = new RemoteCacheManager(
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder().addServer().host("localhost").port(port).build());
      remoteCacheManager.start();
      GlobalComponentRegistry gcr = TestingUtil.extractGlobalComponentRegistry(cacheManager);
      interpreter = gcr.getComponent(Interpreter.class);
   }

   @AfterTest
   public void release() {
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
      TestingUtil.killCacheManagers(cacheManager);
   }

   public void testHotRodCodec() throws Exception {
      Cache<byte[], byte[]> cache = cacheManager.getCache();
      RemoteCache<String, String> remoteCache = remoteCacheManager.getCache();
      remoteCache.put("k1", "v1");
      GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
      byte[] k1 = marshaller.objectToByteBuffer("k1");
      assertTrue(cache.containsKey(k1));

      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      Map<String, String> response = interpreter.execute(sessionId, "get --codec=hotrod k1;");
      assertEquals("v1", response.get(ResultKeys.OUTPUT.toString()));

      assertInterpreter(interpreter.execute(sessionId, "remove --codec=hotrod k1;"));
      String v1 = remoteCache.get("k1");
      assertNull(v1);

      assertInterpreter(interpreter.execute(sessionId, "put --codec=hotrod k2 v2;"));
      String v2 = remoteCache.get("k2");
      assertEquals("v2", v2);

      assertInterpreter(interpreter.execute(sessionId, "evict --codec=hotrod k2;"));
      v2 = remoteCache.get("k2");
      assertNull(v2);
   }

   public void testHotRodEncoding() throws Exception {
      Cache<byte[], byte[]> cache = cacheManager.getCache();
      RemoteCache<String, String> remoteCache = remoteCacheManager.getCache();
      remoteCache.put("k1", "v1");
      GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
      byte[] k1 = marshaller.objectToByteBuffer("k1");
      assertTrue(cache.containsKey(k1));

      String sessionId = interpreter.createSessionId(BasicCacheContainer.DEFAULT_CACHE_NAME);
      interpreter.execute(sessionId, "encoding hotrod;");
      Map<String, String> response = interpreter.execute(sessionId, "get k1;");
      assertEquals("v1", response.get(ResultKeys.OUTPUT.toString()));

      assertInterpreter(interpreter.execute(sessionId, "put k2 v2;"));
      String v2 = remoteCache.get("k2");
      assertEquals("v2", v2);
   }

   private void assertInterpreter(Map<String, String> response) {
      assertNull(response.get(ResultKeys.ERROR.toString()));
   }
}
