package org.infinispan.jmx;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.lang.reflect.Method;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.BackupConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.TestDataSCI;
import org.infinispan.xsite.AbstractMultipleSitesTest;
import org.testng.annotations.Test;

/**
 * Test for cross-site JMX attributes.
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
@Test(groups = "functional", testName = "jmx.XSiteMBeanTest")
public class XSiteMBeanTest extends AbstractMultipleSitesTest {

   private static final int N_SITES = 2;
   private static final int CLUSTER_SIZE = 1;

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   private static void assertSameAttributeAndOperation(MBeanServer mBeanServer, ObjectName objectName,
         Attribute attribute, String site) throws Exception {
      long val1 = invokeLongAttribute(mBeanServer, objectName, attribute);
      log.debugf("%s attr(%s) = %d", objectName, attribute, val1);
      long val2 = invokeLongOperation(mBeanServer, objectName, attribute, site);
      log.debugf("%s op(%s) = %d", objectName, attribute, val2);
      assertEquals("Wrong value for " + attribute, val1, val2);
   }

   private static void assertAttribute(MBeanServer mBeanServer, ObjectName objectName, Attribute attribute,
         long expected) throws Exception {
      long val = invokeLongAttribute(mBeanServer, objectName, attribute);
      log.debugf("%s attr(%s) = %d", objectName, attribute, val);
      assertEquals("Wrong attribute value for " + attribute, expected, val);
   }

   private static void assertHasAttribute(MBeanServer mBeanServer, ObjectName objectName, Attribute attribute)
         throws Exception {
      long val = invokeLongAttribute(mBeanServer, objectName, attribute);
      if (val == -1L) {
         fail("Attribute " + attribute + " expected to be different. " + val + " == -1");
      }
   }

   private static void assertOperation(MBeanServer mBeanServer, ObjectName objectName, Attribute attribute, String site,
         long expected) throws Exception {
      long val = invokeLongOperation(mBeanServer, objectName, attribute, site);
      log.debugf("%s op(%s) = %d", objectName, attribute, val);
      assertEquals("Wrong operation value for " + attribute, expected, val);
   }

   private static long invokeLongOperation(MBeanServer mBeanServer, ObjectName rpcManager, Attribute attribute,
         String siteName)
         throws Exception {
      Object val = mBeanServer
            .invoke(rpcManager, attribute.operationName, new Object[]{siteName}, new String[]{String.class.getName()});
      assertTrue(val instanceof Number);
      return ((Number) val).longValue();
   }

   private static long invokeLongAttribute(MBeanServer mBeanServer, ObjectName rpcManager, Attribute attribute)
         throws Exception {
      Object val = mBeanServer.getAttribute(rpcManager, attribute.attributeName);
      assertTrue(val instanceof Number);
      return ((Number) val).longValue();
   }

   public void testRequestsSent(Method method) throws Exception {
      final String key = k(method);
      final String value = v(method);
      Cache<String, String> cache = cache(0, 0);
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName rpcManager = getRpcManagerObjectName(0);

      assertTrue(mBeanServer.isRegistered(rpcManager));
      resetStats(mBeanServer, rpcManager);

      cache.put(k(method), v(method));
      assertEventuallyInSite(siteName(1), cache1 -> Objects.equals(value, cache1.get(key)), 10, TimeUnit.SECONDS);

      assertAttribute(mBeanServer, rpcManager, Attribute.REQ_SENT, 1);
      assertOperation(mBeanServer, rpcManager, Attribute.REQ_SENT, siteName(1), 1);

      assertHasAttribute(mBeanServer, rpcManager, Attribute.MIN_TIME);
      assertHasAttribute(mBeanServer, rpcManager, Attribute.AVG_TIME);
      assertHasAttribute(mBeanServer, rpcManager, Attribute.MAX_TIME);

      assertSameAttributeAndOperation(mBeanServer, rpcManager, Attribute.MIN_TIME, siteName(1));
      assertSameAttributeAndOperation(mBeanServer, rpcManager, Attribute.AVG_TIME, siteName(1));
      assertSameAttributeAndOperation(mBeanServer, rpcManager, Attribute.MAX_TIME, siteName(1));

      // we only have 1 request, so min==max==avg
      assertEquals(invokeLongAttribute(mBeanServer, rpcManager, Attribute.MIN_TIME),
            invokeLongAttribute(mBeanServer, rpcManager, Attribute.MAX_TIME));
      assertEquals(invokeLongAttribute(mBeanServer, rpcManager, Attribute.MIN_TIME),
            invokeLongAttribute(mBeanServer, rpcManager, Attribute.AVG_TIME));

      resetStats(mBeanServer, rpcManager);
   }

   public void testRequestsReceived(Method method) throws Exception {
      final String key = k(method);
      final String value = v(method);
      Cache<String, String> cache = cache(0, 0);
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      ObjectName rpcManager = getRpcManagerObjectName(1);

      assertTrue(mBeanServer.isRegistered(rpcManager));
      resetStats(mBeanServer, rpcManager);

      cache.put(k(method), v(method));
      assertEventuallyInSite(siteName(1), cache1 -> Objects.equals(value, cache1.get(key)), 10, TimeUnit.SECONDS);

      assertAttribute(mBeanServer, rpcManager, Attribute.REQ_RECV, 1);
      assertOperation(mBeanServer, rpcManager, Attribute.REQ_RECV, siteName(0), 1);

      resetStats(mBeanServer, rpcManager);
   }

   @Override
   protected int defaultNumberOfSites() {
      return N_SITES;
   }

   @Override
   protected int defaultNumberOfNodes() {
      return CLUSTER_SIZE;
   }

   @Override
   protected ConfigurationBuilder defaultConfigurationForSite(int siteIndex) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      for (int i = 0; i < N_SITES; ++i) {
         if (i == siteIndex) {
            //don't add our site as backup.
            continue;
         }
         builder.sites()
                .addBackup()
                .site(siteName(i))
                .strategy(BackupConfiguration.BackupStrategy.ASYNC);
      }
      builder.statistics().enable();
      return builder;
   }

   @Override
   protected GlobalConfigurationBuilder defaultGlobalConfigurationForSite(int siteIndex) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.serialization().addContextInitializer(TestDataSCI.INSTANCE);
      builder.cacheContainer()
             .statistics(true)
             .jmx().enable().domain("xsite-mbean-" + siteIndex).mBeanServerLookup(mBeanServerLookup);
      return builder;
   }

   private void resetStats(MBeanServer mBeanServer, ObjectName rpcManager) throws Exception {
      mBeanServer.invoke(rpcManager, "resetStatistics", new Object[0], new String[0]);
      assertAttribute(mBeanServer, rpcManager, Attribute.REQ_SENT, 0);
      assertAttribute(mBeanServer, rpcManager, Attribute.REQ_RECV, 0);
      assertAttribute(mBeanServer, rpcManager, Attribute.MIN_TIME, -1);
      assertAttribute(mBeanServer, rpcManager, Attribute.AVG_TIME, -1);
      assertAttribute(mBeanServer, rpcManager, Attribute.MAX_TIME, -1);
      for (int i = 0; i < N_SITES; ++i) {
         String site = siteName(i);
         assertOperation(mBeanServer, rpcManager, Attribute.REQ_SENT, site, 0);
         assertOperation(mBeanServer, rpcManager, Attribute.REQ_RECV, site, 0);
         assertOperation(mBeanServer, rpcManager, Attribute.MIN_TIME, site, -1);
         assertOperation(mBeanServer, rpcManager, Attribute.AVG_TIME, site, -1);
         assertOperation(mBeanServer, rpcManager, Attribute.MAX_TIME, site, -1);
      }
   }

   private String getJmxDomain(int siteIndex) {
      return manager(siteIndex, 0).getCacheManagerConfiguration().jmx().domain();
   }

   private ObjectName getRpcManagerObjectName(int siteIndex) {
      return getCacheObjectName(getJmxDomain(siteIndex), getDefaultCacheName() + "(dist_sync)", "RpcManager");
   }

   private enum Attribute {
      REQ_SENT("NumberXSiteRequests", "NumberXSiteRequestsSentTo"),
      REQ_RECV("NumberXSiteRequestsReceived", "NumberXSiteRequestsReceivedFrom"),
      AVG_TIME("AverageXSiteReplicationTime", "AverageXSiteReplicationTimeTo"),
      MAX_TIME("MaximumXSiteReplicationTime", "MaximumXSiteReplicationTimeTo"),
      MIN_TIME("MinimumXSiteReplicationTime", "MinimumXSiteReplicationTimeTo");

      final String attributeName;
      final String operationName;

      Attribute(String attributeName, String operationName) {
         this.attributeName = attributeName;
         this.operationName = operationName;
      }
   }

}
