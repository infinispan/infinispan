package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.remoteCacheManagerObjectName;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.remoteCacheObjectName;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.jmx.PerThreadMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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

   private HotRodServer hotrodServer;
   private CacheContainer cacheContainer;
   private RemoteCacheManager rcm;
   private RemoteCache remoteCache;
   long startTime;

   @BeforeMethod
   protected void setup() throws Exception {
      ConfigurationBuilder cfg = hotRodCacheConfiguration();
      cfg.jmxStatistics().enable();
      cacheContainer = TestCacheManagerFactory
            .createClusteredCacheManagerEnforceJmxDomain(getClass().getSimpleName(), cfg);

      hotrodServer = HotRodClientTestingUtil.startHotRodServer((EmbeddedCacheManager) cacheContainer);
      startTime = System.currentTimeMillis();
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
      clientBuilder.statistics().enable().jmxEnable().jmxDomain(MethodHandles.lookup().lookupClass().getSimpleName()).mBeanServerLookup(new PerThreadMBeanServerLookup());
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
      MBeanServer mbeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName objectName = remoteCacheManagerObjectName(rcm);
      String servers[] = (String[]) mbeanServer.getAttribute(objectName, "Servers");
      assertEquals(1, servers.length);
      assertEquals("localhost:" + hotrodServer.getPort(), servers[0]);
      assertEquals(1, mbeanServer.getAttribute(objectName, "ConnectionCount"));
      assertEquals(1, mbeanServer.getAttribute(objectName, "IdleConnectionCount"));
      assertEquals(0, mbeanServer.getAttribute(objectName, "ActiveConnectionCount"));
   }

   public void testRemoteCacheMBean() throws Exception {
      MBeanServer mbeanServer = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ObjectName objectName = remoteCacheObjectName(rcm, "org.infinispan.default");
      assertEquals(0l, mbeanServer.getAttribute(objectName, "AverageRemoteReadTime"));
      assertEquals(0l, mbeanServer.getAttribute(objectName, "AverageRemoteStoreTime"));
      remoteCache.get("a"); // miss
      assertEquals(1l, mbeanServer.getAttribute(objectName, "RemoteMisses"));
      assertEquals(0l, mbeanServer.getAttribute(objectName, "RemoteHits"));
      assertEquals(0l, mbeanServer.getAttribute(objectName, "RemoteStores"));
      remoteCache.put("a", "a");
      assertEquals(1l, mbeanServer.getAttribute(objectName, "RemoteStores"));
      remoteCache.get("a"); // hit
      assertEquals(1l, mbeanServer.getAttribute(objectName, "RemoteHits"));
      remoteCache.putIfAbsent("a", "a1");
      assertEquals(1l, mbeanServer.getAttribute(objectName, "RemoteStores"));
      remoteCache.putIfAbsent("b", "b");
      assertEquals(2l, mbeanServer.getAttribute(objectName, "RemoteStores"));
      remoteCache.replace("b", "a", "c");
      assertEquals(2l, mbeanServer.getAttribute(objectName, "RemoteStores"));
      remoteCache.replace("b", "b", "c");
      assertEquals(3l, mbeanServer.getAttribute(objectName, "RemoteStores"));

      assertEquals(2, remoteCache.entrySet().stream().count());
      assertEquals(3l, mbeanServer.getAttribute(objectName, "RemoteHits"));

      Map<String, String> map = new HashMap<>(2);
      map.put("c", "c");
      map.put("d", "d");
      remoteCache.putAll(map);
      assertEquals(5l, mbeanServer.getAttribute(objectName, "RemoteStores"));

      Set<String> set = new HashSet<>(3);
      set.add("a");
      set.add("c");
      set.add("e"); // non-existent
      remoteCache.getAll(set);
      assertEquals(5l, mbeanServer.getAttribute(objectName, "RemoteHits"));
      assertEquals(2l, mbeanServer.getAttribute(objectName, "RemoteMisses"));


      assertEquals(0l, mbeanServer.getAttribute(objectName, "RemoteRemoves"));
      remoteCache.remove("b");
      assertEquals(1l, mbeanServer.getAttribute(objectName, "RemoteRemoves"));

      OutputStream os = remoteCache.streaming().put("s");
      os.write('s');
      os.close();
      assertEquals(6l, mbeanServer.getAttribute(objectName, "RemoteStores"));

      InputStream is = remoteCache.streaming().get("s");
      while(is.read() >= 0) {
         //consume
      }
      is.close();
      assertEquals(6l, mbeanServer.getAttribute(objectName, "RemoteHits"));

      assertNull(remoteCache.streaming().get("t"));
      assertEquals(6l, mbeanServer.getAttribute(objectName, "RemoteHits"));
   }

}
