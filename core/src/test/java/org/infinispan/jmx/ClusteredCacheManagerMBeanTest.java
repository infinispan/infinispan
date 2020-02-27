package org.infinispan.jmx;

import static org.infinispan.factories.KnownComponentNames.NON_BLOCKING_EXECUTOR;
import static org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.getCacheManagerObjectName;
import static org.infinispan.test.TestingUtil.getJGroupsChannelObjectName;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.Assert.assertNotEquals;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.executors.LazyInitializingBlockingTaskAwareExecutorService;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Clustered cache manager MBean test
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "jmx.ClusteredCacheManagerMBeanTest")
public class ClusteredCacheManagerMBeanTest extends MultipleCacheManagersTest {

   private static final String JMX_DOMAIN = ClusteredCacheManagerMBeanTest.class.getSimpleName();
   private static final String JMX_DOMAIN2 = JMX_DOMAIN + "2";
   private static final String CACHE_NAME = "mycache";

   private ObjectName name1;
   private ObjectName name2;
   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalConfig1 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfig1, JMX_DOMAIN, mBeanServerLookup);
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
      config.statistics().enable();

      EmbeddedCacheManager cacheManager1 = createClusteredCacheManager(globalConfig1, config, new TransportFlags());
      cacheManager1.start();

      GlobalConfigurationBuilder globalConfig2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      configureJmx(globalConfig2, JMX_DOMAIN2, mBeanServerLookup);
      EmbeddedCacheManager cacheManager2 = createClusteredCacheManager(globalConfig2, config, new TransportFlags());
      cacheManager2.start();

      registerCacheManager(cacheManager1, cacheManager2);
      name1 = getCacheManagerObjectName(JMX_DOMAIN);
      name2 = getCacheManagerObjectName(JMX_DOMAIN2);
      defineConfigurationOnAllManagers(CACHE_NAME, config);
      manager(0).getCache(CACHE_NAME);
      manager(1).getCache(CACHE_NAME);
   }

   public void testAddressInformation() throws Exception {
      MBeanServer server = mBeanServerLookup.getMBeanServer();
      String cm1Address = manager(0).getAddress().toString();
      String cm2Address = manager(1).getAddress().toString();
      assertEquals(cm1Address, server.getAttribute(name1, "NodeAddress"));
      assertTrue(server.getAttribute(name1, "ClusterMembers").toString().contains(cm1Address));
      assertNotEquals("local", server.getAttribute(name1, "PhysicalAddresses"));
      assertEquals(2, server.getAttribute(name1, "ClusterSize"));
      assertEquals(cm2Address, server.getAttribute(name2, "NodeAddress"));
      assertTrue(server.getAttribute(name2, "ClusterMembers").toString().contains(cm2Address));
      assertNotEquals("local", server.getAttribute(name2, "PhysicalAddresses"));
      assertEquals(2, server.getAttribute(name2, "ClusterSize"));
      String cm1members = (String) server.getAttribute(name1, "ClusterMembersPhysicalAddresses");
      assertEquals(2, cm1members.substring(1, cm1members.length() - 2).split(",\\s+").length);
   }

   public void testJGroupsInformation() throws Exception {
      MBeanServer server = mBeanServerLookup.getMBeanServer();
      ObjectName jchannelName1 = getJGroupsChannelObjectName(manager(0));
      ObjectName jchannelName2 = getJGroupsChannelObjectName(manager(1));
      assertEquals(server.getAttribute(name1, "NodeAddress"), server.getAttribute(jchannelName1, "address"));
      assertEquals(server.getAttribute(name2, "NodeAddress"), server.getAttribute(jchannelName2, "address"));
      assertTrue((Boolean) server.getAttribute(jchannelName1, "connected"));
      assertTrue((Boolean) server.getAttribute(jchannelName2, "connected"));
   }

   public void testExecutorMBeans() throws Exception {
      MBeanServer server = mBeanServerLookup.getMBeanServer();
      ObjectName objectName = getCacheManagerObjectName(JMX_DOMAIN, "DefaultCacheManager", TIMEOUT_SCHEDULE_EXECUTOR);
      assertTrue(server.isRegistered(objectName));
      assertEquals(Integer.MAX_VALUE, server.getAttribute(objectName, "MaximumPoolSize"));
      String javaVersion = System.getProperty("java.version");
      assertEquals(javaVersion.startsWith("1.8.") ? 0L : 10L, server.getAttribute(objectName, "KeepAliveTime"));

      LazyInitializingBlockingTaskAwareExecutorService remoteExecutor =
         extractGlobalComponent(manager(0), LazyInitializingBlockingTaskAwareExecutorService.class,
                                NON_BLOCKING_EXECUTOR);
      remoteExecutor.submit(() -> {});

      objectName = getCacheManagerObjectName(JMX_DOMAIN, "DefaultCacheManager", NON_BLOCKING_EXECUTOR);
      assertTrue(server.isRegistered(objectName));
      assertEquals(30000L, server.getAttribute(objectName, "KeepAliveTime"));
      assertEquals(TestCacheManagerFactory.NAMED_EXECUTORS_THREADS_NO_QUEUE,
                   server.getAttribute(objectName, "MaximumPoolSize"));
   }
}
