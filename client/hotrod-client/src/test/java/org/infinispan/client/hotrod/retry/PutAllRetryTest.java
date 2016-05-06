package org.infinispan.client.hotrod.retry;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

import java.util.TreeMap;

import static java.util.stream.IntStream.range;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

@Test(testName = "client.hotrod.retry.PutAllRetryTest", groups = "functional")
public class PutAllRetryTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(3, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2);
      return hotRodCacheConfiguration(builder);
   }

   @Override
   protected int maxRetries() {
      return 10;
   }

   @Test
   public void testFailOver() throws InterruptedException {
      RemoteCache<Integer, String> remoteCache = clients.get(0).getCache();

      int size = 1000;

      TreeMap<Integer, String> dataMap = new TreeMap<>();
      range(0, size).forEach(num -> dataMap.put(num, "value" + num));

      remoteCache.putAll(dataMap.subMap(0, size / 2));

      HotRodClientTestingUtil.killServers(servers.get(0));

      remoteCache.putAll(dataMap.subMap(size / 2, size));

      assertEquals(size, remoteCache.size());
   }

}
