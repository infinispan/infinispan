package org.infinispan.jmx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import static org.infinispan.test.TestingUtil.*;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Clustered cache manager MBean test
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "jmx.ClusteredCacheManagerMBeanTest")
public class ClusteredCacheManagerMBeanTest extends MultipleCacheManagersTest {

   public static final String JMX_DOMAIN = ClusteredCacheManagerMBeanTest.class.getSimpleName();
   public static final String JMX_DOMAIN2 = JMX_DOMAIN + "2";

   private ObjectName name1;
   private ObjectName name2;
   private MBeanServer server;

   @Override
   protected void createCacheManagers() throws Throwable {
      CacheContainer cacheManager1 = TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(JMX_DOMAIN, true);
      cacheManager1.start();
      CacheContainer cacheManager2 = TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(JMX_DOMAIN, true);
      cacheManager2.start();
      registerCacheManager(cacheManager1, cacheManager2);
      name1 = getCacheManagerObjectName(JMX_DOMAIN);
      name2 = getCacheManagerObjectName(JMX_DOMAIN2);
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
      config.jmxStatistics().enable();
      defineConfigurationOnAllManagers("mycache", config);
      manager(0).getCache("mycache");
      manager(1).getCache("mycache");
   }

   public void testAddressInformation() throws Exception {
      String cm1Address = manager(0).getAddress().toString();
      String cm2Address = manager(1).getAddress().toString();
      assert server.getAttribute(name1, "NodeAddress").equals(cm1Address);
      assert server.getAttribute(name1, "ClusterMembers").toString().contains(cm1Address);
      assert !server.getAttribute(name1, "PhysicalAddresses").toString().equals("local");
      assert server.getAttribute(name1, "ClusterSize").equals(2);
      assert server.getAttribute(name2, "NodeAddress").equals(cm2Address);
      assert server.getAttribute(name2, "ClusterMembers").toString().contains(cm2Address);
      assert !server.getAttribute(name2, "PhysicalAddresses").toString().equals("local");
      assert server.getAttribute(name2, "ClusterSize").equals(2);
   }

   public void testJGroupsInformation() throws Exception {
      ObjectName jchannelName1 = getJGroupsChannelObjectName(JMX_DOMAIN, manager(0).getClusterName());
      ObjectName jchannelName2 = getJGroupsChannelObjectName(JMX_DOMAIN2, manager(1).getClusterName());
      assertEquals(server.getAttribute(name1, "NodeAddress"), server.getAttribute(jchannelName1, "address"));
      assertEquals(server.getAttribute(name2, "NodeAddress"), server.getAttribute(jchannelName2, "address"));
      assert (Boolean) server.getAttribute(jchannelName1, "connected");
      assert (Boolean) server.getAttribute(jchannelName2, "connected");
   }
}
