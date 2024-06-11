package org.infinispan.client.hotrod.stress;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * A simple test that stresses a local HotRod server.
 *
 * @author Dan Berindei
 * @since 8.1
 */
@Test(groups = "profiling", testName = "client.hotrod.profiling.HotRodLocalProfilingTest")
public class HotRodLocalProfilingTest extends SingleCacheManagerTest {
   public void testPutBigSizeValue() {
      System.out.println("Starting test");
      long nanos = System.nanoTime();
      HotRodServer hotRodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      String servers = HotRodClientTestingUtil.getServersString(hotRodServer);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServers(servers);
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(clientBuilder.build());
      RemoteCache<Object, Object> remoteCache = remoteCacheManager.getCache();

      for (int i = 0; i < 10000000; i++) {
         byte[] key = ("key" + i).getBytes();
         byte[] value = ("value" + i).getBytes();
         remoteCache.put(key, value);
         if ((i & 0xFFFF) == 0xFFFF) {
            System.out.println("Written " + i + " entries.");
         }
      }
      System.out.println("Test took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nanos) + "ms.");
   }

   @AfterMethod
   @Override
   protected void clearContent() {
      // Do nothing
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder configuration = new ConfigurationBuilder();
      configuration.clustering().cacheMode(CacheMode.REPL_SYNC);
      return TestCacheManagerFactory.createClusteredCacheManager(configuration);
   }
}
