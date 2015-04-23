package org.infinispan.expiration.impl;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * Tests eviction thread counts under several distinct circumstances.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "eviction.ExpirationThreadCountTest")
public class ExpirationThreadCountTest extends SingleCacheManagerTest {

   private static String EXPIRATION_THREAD_NAME_PREFIX = ExpirationThreadCountTest.class.getSimpleName() + "-thread";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder globalCfg = new GlobalConfigurationBuilder();
      globalCfg.expirationThreadPool().threadFactory(new DefaultThreadFactory(null, 1, EXPIRATION_THREAD_NAME_PREFIX, null, null));
      return TestCacheManagerFactory.createCacheManager(globalCfg, new ConfigurationBuilder());
   }

   public void testDefineMultipleCachesWithExpiration() {
      for (int i = 0; i < 50; i++) {
         ConfigurationBuilder cfg = new ConfigurationBuilder();
         cfg
            .expiration().wakeUpInterval(100L);

         String cacheName = Integer.toString(i);
         cacheManager.defineConfiguration(cacheName, cfg.build());
         cacheManager.getCache(cacheName);
      }

      ThreadMXBean threadMBean = ManagementFactory.getThreadMXBean();
      ThreadInfo[] threadInfos = threadMBean.dumpAllThreads(false, false);

      String pattern = EXPIRATION_THREAD_NAME_PREFIX;
      int evictionThreadCount = 0;
      for (ThreadInfo threadInfo : threadInfos) {
         if (threadInfo.getThreadName().startsWith(pattern))
            evictionThreadCount++;
      }

      assert evictionThreadCount == 1 : "Thread should only be one expiration thread with pattern '"
            + pattern + "', instead there were " + evictionThreadCount;
   }

}
