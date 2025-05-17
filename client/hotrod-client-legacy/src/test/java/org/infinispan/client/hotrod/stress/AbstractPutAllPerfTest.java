package org.infinispan.client.hotrod.stress;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
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
 * Tests putAll performance with large and small data sets
 *
 * @author William Burns
 * @since 7.2
 */
@Test(groups = "stress")
public abstract class AbstractPutAllPerfTest extends MultipleCacheManagersTest {

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

   protected void runTest(int size, String name) {
      long begin = System.currentTimeMillis();
      int iterations = 0;
      Map<Integer, Integer> map = new HashMap<Integer, Integer>();
      Random random = new Random();
      long currentTime;
      while (millisecondsToRun + begin > (currentTime = System.currentTimeMillis())) {
         map.clear();
         for (int i = 0; i < size; ++i) {
            int value = random.nextInt(Integer.MAX_VALUE);
            map.put(value, value);
         }
         remoteCache.putAll(map);
         iterations++;
      }
      long totalTime = currentTime - begin;
      System.out.println(name + " - Performed " + iterations + " in " + totalTime + " ms generating " +
            iterations / (totalTime / 1000)  + " ops/sec");
   }

   public void test5Input() {
      runTest(5, "test5Input");
   }

   public void test500Input() {
      runTest(500, "test500Input");
   }

   public void test50000Input() {
      runTest(50000, "test50000Input");
   }
}
