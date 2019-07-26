package org.infinispan.server.hotrod;

import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestErrorResponse;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests stats operation against a Hot Rod server.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodNoDefaultCacheTest")
public class HotRodNoDefaultCacheTest extends SingleCacheManagerTest {

   private HotRodServer server;
   private HotRodClient client;

   @Override
   public EmbeddedCacheManager createCacheManager() {
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager((ConfigurationBuilder) null);
      return cm;
   }

   @Override
   protected void setup() throws Exception {
      // super() creates the default cache and we do not want that
      cacheManager = createCacheManager();
      server = startHotRodServer(cacheManager, (String)null);
      client = new HotRodClient("127.0.0.1", server.getPort(), "", 60, HotRodVersion.HOTROD_21.getVersion());
   }

   @Override
   protected void teardown() {
      log.debug("Killing Hot Rod client and server");
      killClient(client);
      killServer(server);
      super.teardown();
   }

   public void testNoDefault() {
      TestResponse response = client.get("k1");
      assertTrue(response instanceof TestErrorResponse);
      TestErrorResponse error = (TestErrorResponse)response;
      assertEquals("org.infinispan.server.hotrod.CacheNotFoundException: Default cache requested but not configured", error.msg);
   }
}
