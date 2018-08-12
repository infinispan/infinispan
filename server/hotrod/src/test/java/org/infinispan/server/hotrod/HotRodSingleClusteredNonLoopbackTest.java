package org.infinispan.server.hotrod;

import static org.infinispan.server.core.test.ServerTestingUtil.killServer;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.findNetworkInterfaces;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.getDefaultHotRodConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.Method;
import java.net.NetworkInterface;
import java.util.EnumSet;
import java.util.List;

import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.HotRodSingleClusteredNonLoopbackTest")
public class HotRodSingleClusteredNonLoopbackTest extends MultipleCacheManagersTest {

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
      List<NetworkInterface> nonLoopInterfaces = findNetworkInterfaces(false);
      SkipTestNG.skipIf(nonLoopInterfaces.isEmpty(), "No non-loop network interface");
      NetworkInterface iface = nonLoopInterfaces.iterator().next();
      String address = iface.getInetAddresses().nextElement().getHostAddress();
      hotRodServer = startHotRodServer(cacheManagers.get(0), address, serverPort(), 0, getDefaultHotRodConfiguration());
      hotRodClient = new HotRodClient(address, hotRodServer.getPort(), cacheName, 60, (byte) 20);
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      log.debug("Test finished, close client, server, and cache managers");
      killClient(hotRodClient);
      killServer(hotRodServer);
      super.destroy();
   }

   public void testNonLoopbackPutOnProtectedCache(Method m) {
      InternalCacheRegistry internalCacheRegistry =
            manager(0).getGlobalComponentRegistry().getComponent(InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache("MyInternalCache",
                                                  new ConfigurationBuilder().build(),
                                                  EnumSet.of(InternalCacheRegistry.Flag.USER,
                                                             InternalCacheRegistry.Flag.PROTECTED));
      TestResponse resp = hotRodClient
            .execute(0xA0, (byte) 0x01, "MyInternalCache", k(m), 0, 0, v(m), 0, (byte) 1, 0);
      assertEquals(resp.status, Success, "Status should have been 'Success' but instead was: " + resp.status);
      hotRodClient.assertPut(m);
   }


}
