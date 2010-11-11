package org.infinispan.eviction;

import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Properties;

/**
 * Tests eviction thread counts under several distinct circumstances.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "eviction.EvictionThreadCountTest")
public class EvictionThreadCountTest extends SingleCacheManagerTest {

   private static String EVICT_THREAD_NAME_PREFIX = EvictionThreadCountTest.class.getSimpleName() + "-thread";

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfiguration globalCfg = new GlobalConfiguration();
      Properties props = new Properties();
      props.setProperty("threadNamePrefix", EVICT_THREAD_NAME_PREFIX);
      globalCfg.setEvictionScheduledExecutorProperties(props);
      return TestCacheManagerFactory.createCacheManager(globalCfg);
   }

   public void testDefineMultipleCachesWithEviction() {
      for (int i = 0; i < 50; i++) {
         Configuration cfg = new Configuration();
         cfg.setEvictionStrategy(EvictionStrategy.LIRS);
         cfg.setEvictionWakeUpInterval(100);
         cfg.setEvictionMaxEntries(128); // 128 max entries
         cfg.setUseLockStriping(false); // to minimize chances of deadlock in the unit test
         String cacheName = Integer.toString(i);
         cacheManager.defineConfiguration(cacheName, cfg);
         cacheManager.getCache(cacheName);
      }

      ThreadMXBean threadMBean = ManagementFactory.getThreadMXBean();
      ThreadInfo[] threadInfos = threadMBean.dumpAllThreads(false, false);

      String pattern = "Scheduled-" + EVICT_THREAD_NAME_PREFIX;
      int evictionThreadCount = 0;
      for (ThreadInfo threadInfo : threadInfos) {
         if (threadInfo.getThreadName().startsWith(pattern))
            evictionThreadCount++;
      }

      assert evictionThreadCount == 1 : "Thread should only be one eviction thread with pattern '"
            + pattern + "', instead there were " + evictionThreadCount;
   }

}
