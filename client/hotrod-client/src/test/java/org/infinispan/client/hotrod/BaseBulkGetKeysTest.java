package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests functionality related to getting multiple entries from a HotRod server
 * in bulk.
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 */
@Test(groups = "functional")
public abstract class BaseBulkGetKeysTest extends MultipleCacheManagersTest {

   protected HotRodServer[] hotrodServers;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;

   abstract protected int numberOfHotRodServers();

   abstract protected ConfigurationBuilder clusterConfig();

   @Override
   protected void createCacheManagers() throws Throwable {
      final int numServers = numberOfHotRodServers();
      hotrodServers = new HotRodServer[numServers];

      createCluster(hotRodCacheConfiguration(clusterConfig()), numberOfHotRodServers());

      for (int i = 0; i < numServers; i++) {
         EmbeddedCacheManager cm = cacheManagers.get(i);
         hotrodServers[i] = TestHelper.startHotRodServer(cm);
      }

      String servers = TestHelper.getServersString(hotrodServers);

      remoteCacheManager = new RemoteCacheManager(servers);
      remoteCache = remoteCacheManager.getCache();
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServers);
   }

   protected void populateCacheManager() {
      for (int i = 0; i < 100; i++) {
         remoteCache.put(i, i);
      }
   }

   public void testBulkGetKeys() {
      populateCacheManager();
      Set<Object> set = remoteCache.keySet();
      assert set.size() == 100;
      for (int i = 0; i < 100; i++) {
         assert set.contains(i);
      }
   }

   public void testBulkGetAfterLifespanExpire() throws InterruptedException {
      Map<String, String> dataIn = new HashMap<String, String>();
      dataIn.put("aKey", "aValue");
      dataIn.put("bKey", "bValue");
      final long startTime = System.currentTimeMillis();
      final long lifespan = 10000;
      remoteCache.putAll(dataIn, lifespan, TimeUnit.MILLISECONDS);

      Set<Object> dataOut = new HashSet<Object>();
      while (true) {
         dataOut = remoteCache.keySet();
         if (System.currentTimeMillis() >= startTime + lifespan)
            break;
         assert dataOut.size() == dataIn.size() : String
               .format("Data size not the same, put in %s elements, keySet has %s elements",
                     dataIn.size(), dataOut.size());
         for (Object outKey : dataOut) {
            assert dataIn.containsKey(outKey);
         }
         Thread.sleep(100);
      }

      // Make sure that in the next 30 secs data is removed
      while (System.currentTimeMillis() < startTime + lifespan + 30000) {
         dataOut = remoteCache.keySet();
         if (dataOut.size() == 0)
            return;
      }

      assert dataOut.size() == 0 : String.format(
            "Data not empty, it contains: %s elements", dataOut.size());
   }

}
