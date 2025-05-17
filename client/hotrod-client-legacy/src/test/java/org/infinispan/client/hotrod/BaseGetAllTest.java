package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests functionality related to getting multiple entries from a HotRod server
 * using getAll method.
 *
 * @author William Burns
 * @since 7.2
 */
@Test(groups = "functional")
public abstract class BaseGetAllTest extends MultipleCacheManagersTest {

   protected HotRodServer[] hotrodServers;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;

   protected abstract int numberOfHotRodServers();

   protected abstract ConfigurationBuilder clusterConfig();

   @Override
   protected void createCacheManagers() throws Throwable {
      final int numServers = numberOfHotRodServers();
      hotrodServers = new HotRodServer[numServers];

      createCluster(hotRodCacheConfiguration(clusterConfig()), numberOfHotRodServers());

      for (int i = 0; i < numServers; i++) {
         EmbeddedCacheManager cm = cacheManagers.get(i);
         hotrodServers[i] = HotRodClientTestingUtil.startHotRodServer(cm);
      }

      String servers = HotRodClientTestingUtil.getServersString(hotrodServers);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServers(servers);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServers);
      hotrodServers = null;
   }

   protected Set<Integer> populateCacheManager() {
      Map<Integer, Integer> entries = new HashMap<Integer, Integer>();
      for (int i = 0; i < 100; i++) {
         entries.put(i, i);
      }
      remoteCache.putAll(entries);
      return entries.keySet();
   }

   public void testBulkGetKeys() {
      Set<Integer> keys = populateCacheManager();
      Map<Object, Object> map = remoteCache.getAll(keys);
      assertEquals(100, map.size());
      for (int i = 0; i < 100; i++) {
         assertEquals(i, map.get(i));
      }
   }

   public void testBulkGetAfterLifespanExpire() throws InterruptedException {
      Map<String, String> dataIn = new HashMap<String, String>();
      dataIn.put("aKey", "aValue");
      dataIn.put("bKey", "bValue");
      final long startTime = System.currentTimeMillis();
      final long lifespan = 10000;
      remoteCache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      Map<Object, Object> dataOut = new HashMap<Object, Object>();
      while (true) {
         dataOut = remoteCache.getAll(dataIn.keySet());
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         assertEquals(dataOut.size(), dataIn.size());
         for (Entry<Object, Object> outEntry : dataOut.entrySet()) {
            assertEquals(dataIn.get(outEntry.getKey()), outEntry.getValue());
         }
         Thread.sleep(100);
      }

      int size = dataOut.size();
      // Make sure that in the next 30 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 30000) {
         dataOut = remoteCache.getAll(dataIn.keySet());
         if ((size = dataOut.size()) == 0) {
            break;
         }
      }

      assertEquals("There shouldn't be any values left!", 0, size);
   }

}
