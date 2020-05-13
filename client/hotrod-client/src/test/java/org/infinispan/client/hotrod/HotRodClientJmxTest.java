package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.remoteCacheManagerObjectName;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.remoteCacheObjectName;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.HotRodClientJmxTest")
public class HotRodClientJmxTest extends AbstractInfinispanTest {

   private static final String JMX_DOMAIN = HotRodClientJmxTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   private HotRodServer hotrodServer;
   private CacheContainer cacheContainer;
   private RemoteCacheManager rcm;
   private RemoteCache<String, String> remoteCache;

   @BeforeMethod
   void setup() {
      ConfigurationBuilder cfg = hotRodCacheConfiguration();
      cfg.statistics().enable();
      GlobalConfigurationBuilder globalCfg = GlobalConfigurationBuilder.defaultClusteredBuilder();
      TestCacheManagerFactory.configureJmx(globalCfg, JMX_DOMAIN, mBeanServerLookup);
      cacheContainer = TestCacheManagerFactory.createClusteredCacheManager(globalCfg, cfg);

      hotrodServer = HotRodClientTestingUtil.startHotRodServer((EmbeddedCacheManager) cacheContainer);

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
      clientBuilder.statistics()
            .enable()
            .jmxEnable()
            .jmxDomain(JMX_DOMAIN)
            .mBeanServerLookup(mBeanServerLookup);
      rcm = new RemoteCacheManager(clientBuilder.build());
      remoteCache = rcm.getCache();
   }

   @AfterMethod
   void tearDown() {
      TestingUtil.killCacheManagers(cacheContainer);
      killRemoteCacheManager(rcm);
      killServers(hotrodServer);
   }

   public void testRemoteCacheManagerMBean() throws Exception {
      MBeanServer mbeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName objectName = remoteCacheManagerObjectName(rcm);
      String[] servers = (String[]) mbeanServer.getAttribute(objectName, "Servers");
      assertEquals(1, servers.length);
      assertEquals("localhost:" + hotrodServer.getPort(), servers[0]);
      assertEquals(1, mbeanServer.getAttribute(objectName, "ConnectionCount"));
      assertEquals(1, mbeanServer.getAttribute(objectName, "IdleConnectionCount"));
      assertEquals(0, mbeanServer.getAttribute(objectName, "ActiveConnectionCount"));
   }

   public void testRemoteCacheMBean() throws Exception {
      MBeanServer mbeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName objectName = remoteCacheObjectName(rcm, "org.infinispan.default");
      assertEquals(0L, mbeanServer.getAttribute(objectName, "AverageRemoteReadTime"));
      assertEquals(0L, mbeanServer.getAttribute(objectName, "AverageRemoteStoreTime"));
      remoteCache.get("a"); // miss
      assertEquals(1L, mbeanServer.getAttribute(objectName, "RemoteMisses"));
      assertEquals(0L, mbeanServer.getAttribute(objectName, "RemoteHits"));
      assertEquals(0L, mbeanServer.getAttribute(objectName, "RemoteStores"));
      remoteCache.put("a", "a");
      assertEquals(1L, mbeanServer.getAttribute(objectName, "RemoteStores"));
      assertEquals("a", remoteCache.get("a")); // hit
      assertEquals(1L, mbeanServer.getAttribute(objectName, "RemoteHits"));
      remoteCache.putIfAbsent("a", "a1");
      assertEquals(1L, mbeanServer.getAttribute(objectName, "RemoteStores"));
      assertNull(remoteCache.putIfAbsent("b", "b"));
      assertEquals(2L, mbeanServer.getAttribute(objectName, "RemoteStores"));
      assertFalse(remoteCache.replace("b", "a", "c"));
      assertEquals(2L, mbeanServer.getAttribute(objectName, "RemoteStores"));
      assertTrue(remoteCache.replace("b", "b", "c"));
      assertEquals(3L, mbeanServer.getAttribute(objectName, "RemoteStores"));

      assertEquals(2, remoteCache.entrySet().stream().count());
      assertEquals(3L, mbeanServer.getAttribute(objectName, "RemoteHits"));

      Map<String, String> map = new HashMap<>(2);
      map.put("c", "c");
      map.put("d", "d");
      remoteCache.putAll(map);
      assertEquals(5L, mbeanServer.getAttribute(objectName, "RemoteStores"));

      Set<String> set = new HashSet<>(3);
      set.add("a");
      set.add("c");
      set.add("e"); // non-existent
      remoteCache.getAll(set);
      assertEquals(5L, mbeanServer.getAttribute(objectName, "RemoteHits"));
      assertEquals(2L, mbeanServer.getAttribute(objectName, "RemoteMisses"));

      assertEquals(0L, mbeanServer.getAttribute(objectName, "RemoteRemoves"));
      remoteCache.remove("b");
      assertEquals(1L, mbeanServer.getAttribute(objectName, "RemoteRemoves"));

      OutputStream os = remoteCache.streaming().put("s");
      os.write('s');
      os.close();
      assertEquals(6L, mbeanServer.getAttribute(objectName, "RemoteStores"));

      InputStream is = remoteCache.streaming().get("s");
      while (is.read() >= 0) {
         //consume
      }
      is.close();
      assertEquals(6L, mbeanServer.getAttribute(objectName, "RemoteHits"));

      assertNull(remoteCache.streaming().get("t"));
      assertEquals(6L, mbeanServer.getAttribute(objectName, "RemoteHits"));
   }
}
