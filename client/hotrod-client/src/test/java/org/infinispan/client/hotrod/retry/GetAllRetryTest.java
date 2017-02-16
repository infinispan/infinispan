package org.infinispan.client.hotrod.retry;

import static java.util.stream.IntStream.range;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

@Test(testName = "client.hotrod.retry.GetAllRetryTest", groups = "functional")
public class GetAllRetryTest extends MultiHotRodServersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(3, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }

   @Override
   protected int maxRetries() {
      return 10;
   }

   @Test
   public void testFailOver() throws InterruptedException {
      RemoteCache<Integer, String> remoteCache = clients.get(0).getCache();

      int size = 1000;

      range(0, size).forEach(num -> remoteCache.put(num, "value" + num));

      Map<Integer, String> firstBatch = remoteCache.getAll(range(0, size / 2).boxed().collect(Collectors.toSet()));

      HotRodClientTestingUtil.killServers(servers.get(0));

      Map<Integer, String> secondBatch = remoteCache.getAll(range(size / 2, size).boxed().collect(Collectors.toSet()));

      assertEquals(size, firstBatch.size() + secondBatch.size());
   }

}
