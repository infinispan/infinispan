package org.infinispan.client.hotrod.stress;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Tests getAll performance with large and small data sets
 *
 * @author William Burns
 * @since 7.2
 */
@Test(groups = "stress")
public abstract class AbstractGetAllPerfTest extends MultipleCacheManagersTest {

   protected HotRodServer[] hotrodServers;
   protected RemoteCacheManager remoteCacheManager;
   protected RemoteCache<Object, Object> remoteCache;

   protected abstract int numberOfHotRodServers();

   protected abstract ConfigurationBuilder clusterConfig();

   protected final long millisecondsToRun = TimeUnit.MINUTES.toMillis(1);

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
      remoteCacheManager = null;
      killServers(hotrodServers);
      hotrodServers = null;
   }

   protected void runTest(int size, int possibilities, String name) {
      assertTrue(possibilities > size);
      Map<Integer, Integer> map = new HashMap<Integer, Integer>();
      for (int i = 0; i < size; ++i) {
         map.put(i, i);
      }
      remoteCache.putAll(map);
      long begin = System.currentTimeMillis();
      int iterations = 0;
      Set<Integer> set = new HashSet<Integer>();
      Random random = new Random();
      long currentTime;
      while (millisecondsToRun + begin > (currentTime = System.currentTimeMillis())) {
         int count = 0;
         set.clear();
         for (int i = 0; i < size;) {
            int value = random.nextInt(possibilities);
            if (set.add(value)) {
               i++;
               if (value < size) {
                  count++;
               }
            }
         }
         assertEquals(count, remoteCache.getAll(set).size());
         iterations++;
      }
      long totalTime = currentTime - begin;
      System.out.println(name + " - Performed " + iterations + " in " + totalTime + " ms generating " +
            iterations / (totalTime / 1000)  + " ops/sec");
   }

   public void test5Input() {
      runTest(5, 8, "test5Input");
   }

   public void test500Input() {
      runTest(500, 800, "test500Input");
   }

   public void test50000Input() {
      runTest(50000, 80000, "test50000Input");
   }
}
