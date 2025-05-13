package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.stats.CacheContainerStats;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "jmx.CacheContainerStatsMBeanTest")
public class CacheContainerStatsMBeanTest extends MultipleCacheManagersTest {

   private final String cachename = CacheContainerStatsMBeanTest.class.getName();
   private final String cachename2 = cachename + "2";
   private static final String JMX_DOMAIN = CacheContainerStatsMBeanTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   private ControlledTimeService timeService;

   @Override
   protected void createCacheManagers() throws Throwable {
      timeService = new ControlledTimeService();
      ConfigurationBuilder defaultConfig = new ConfigurationBuilder();
      GlobalConfigurationBuilder gcb1 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb1.cacheContainer().statistics(true)
          .jmx().enabled(true).domain(JMX_DOMAIN).mBeanServerLookup(mBeanServerLookup)
            .metrics().accurateSize(true);
      CacheContainer cacheManager1 = TestCacheManagerFactory.createClusteredCacheManager(gcb1, defaultConfig,
            new TransportFlags());
      TestingUtil.replaceComponent(cacheManager1, TimeService.class, timeService, true);
      cacheManager1.start();

      GlobalConfigurationBuilder gcb2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb2.cacheContainer().statistics(true)
          .jmx().enabled(true).domain(JMX_DOMAIN + 2).mBeanServerLookup(mBeanServerLookup);
      CacheContainer cacheManager2 = TestCacheManagerFactory.createClusteredCacheManager(gcb2, defaultConfig,
            new TransportFlags());
      TestingUtil.replaceComponent(cacheManager2, TimeService.class, timeService, true);
      cacheManager2.start();

      registerCacheManager(cacheManager1, cacheManager2);

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.REPL_SYNC).statistics().enable();
      defineConfigurationOnAllManagers(cachename, cb);
      defineConfigurationOnAllManagers(cachename2, cb);
      waitForClusterToForm(cachename);
   }

   public void testClusterStats() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName nodeStats = getCacheManagerObjectName(JMX_DOMAIN, "DefaultCacheManager",
            CacheContainerStats.OBJECT_NAME);
      mBeanServer.setAttribute(nodeStats, new Attribute("StatisticsEnabled", Boolean.TRUE));
   }

   public void testClusterStatsDisabled() throws Exception {
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName nodeStats = getCacheManagerObjectName(JMX_DOMAIN, "DefaultCacheManager",
            CacheContainerStats.OBJECT_NAME);
      mBeanServer.setAttribute(nodeStats, new Attribute("StatisticsEnabled", Boolean.FALSE));
   }
}
