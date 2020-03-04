package org.infinispan.server.hotrod;

import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.OperationStatus.ParseError;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.EnumSet;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestErrorResponse;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.HotRodSingleClusteredTest")
public class HotRodSingleClusteredTest extends MultipleCacheManagersTest {

   private HotRodServer hotRodServer;
   private HotRodClient hotRodClient;
   private String cacheName = "HotRodCache";

   @Override
   protected void createCacheManagers() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration());
      cacheManagers.add(cm);
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      cm.defineConfiguration(cacheName, builder.build());

   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      hotRodServer = startHotRodServer(cacheManagers.get(0));
      hotRodClient = new HotRodClient("127.0.0.1", hotRodServer.getPort(), cacheName, 60, (byte) 20);
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      log.debug("Test finished, close client, server, and cache managers");
      killClient(hotRodClient);
      killServer(hotRodServer);
      super.destroy();
   }

   public void testPutGet(Method m) {
      assertStatus(hotRodClient.put(k(m), 0, 0, v(m)), Success);
      assertSuccess(hotRodClient.get(k(m), 0), v(m));
   }

   public void testPutOnPrivateCache(Method m) {
      TestErrorResponse resp = (TestErrorResponse) hotRodClient
            .execute(0xA0, (byte) 0x01, hotRodServer.getConfiguration().topologyCacheName(), k(m), 0, 0, v(m), 0,
                     (byte) 1, 0);
      assertTrue(resp.msg.contains("Remote requests are not allowed to private caches."));
      assertEquals("Status should have been 'ParseError' but instead was: " + resp.status, ParseError, resp.status);
      hotRodClient.assertPut(m);
   }

   public void testLoopbackPutOnProtectedCache(Method m) {
      InternalCacheRegistry internalCacheRegistry =
            manager(0).getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache("MyInternalCache",
                                                  hotRodCacheConfiguration().build(),
                                                  EnumSet.of(InternalCacheRegistry.Flag.USER,
                                                             InternalCacheRegistry.Flag.PROTECTED));
      TestResponse resp = hotRodClient.execute(0xA0, (byte) 0x01, "MyInternalCache", k(m), 0, 0, v(m), 0, (byte) 1, 0);
      assertEquals(Success, resp.status);
   }


}
