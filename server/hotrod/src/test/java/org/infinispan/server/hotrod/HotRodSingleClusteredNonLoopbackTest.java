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
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.HotRodSingleClusteredNonLoopbackTest")
public class HotRodSingleClusteredNonLoopbackTest extends MultipleCacheManagersTest {

   private HotRodServer hotRodServer;
   private HotRodClient hotRodClient;
   private final String cacheName = "HotRodCache";

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
      log.debugf("Found network interfaces %s", nonLoopInterfaces);
      for (NetworkInterface iface : nonLoopInterfaces) {
         String address = iface.getInetAddresses().nextElement().getHostAddress();
         try {
            hotRodServer = startHotRodServer(cacheManagers.get(0), address, serverPort(), getDefaultHotRodConfiguration());
            hotRodClient = new HotRodClient(address, hotRodServer.getPort(), cacheName, (byte) 20);
            return;
         } catch (Throwable t) {
            if (!t.getMessage().contains("Address family not supported by protocol")) {
               throw new RuntimeException(t);
            }
         }
      }
      throw new SkipException("Could not find a valid interface for this test among " + nonLoopInterfaces);
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
            GlobalComponentRegistry.componentOf(manager(0), InternalCacheRegistry.class);
      internalCacheRegistry.registerInternalCache("MyInternalCache",
                                                  hotRodCacheConfiguration().build(),
                                                  EnumSet.of(InternalCacheRegistry.Flag.USER,
                                                             InternalCacheRegistry.Flag.PROTECTED));
      TestResponse resp = hotRodClient
            .execute(0xA0, (byte) 0x01, "MyInternalCache", k(m), 0, 0, v(m), 0, (byte) 1, 0);
      assertEquals(resp.status, Success, "Status should have been 'Success' but instead was: " + resp.status);
      hotRodClient.assertPut(m);
   }


}
