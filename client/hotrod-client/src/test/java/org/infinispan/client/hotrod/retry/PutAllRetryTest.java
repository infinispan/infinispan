package org.infinispan.client.hotrod.retry;

import static java.util.stream.IntStream.range;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.blockUntilViewReceived;
import static org.testng.AssertJUnit.assertEquals;

import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

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

   @Test(invocationCount = 3)
   public void testFailOver() throws InterruptedException, ExecutionException, TimeoutException {
      RemoteCache<Integer, String> remoteCache = clients.get(0).getCache();

      int size = 10;

      TreeMap<Integer, String> dataMap = new TreeMap<>();
      range(0, size).forEach(num -> dataMap.put(num, "value" + num));

      CompletableFuture<Void> stage = remoteCache.putAllAsync(dataMap.subMap(0, size / 2));

      HotRodClientTestingUtil.killServers(servers.get(0));
      killMember(0);

      stage.get(10, TimeUnit.SECONDS);

      blockUntilViewReceived(manager(1).getCache(), 2, 10000, false);

      stage = remoteCache.putAllAsync(dataMap.subMap(size / 2, size));

      addHotRodServer(getCacheConfiguration());

      blockUntilViewReceived(manager(2).getCache(), 3, 10000, false);

      stage.get(10, TimeUnit.SECONDS);

      assertEquals(size, remoteCache.size());
   }

}
