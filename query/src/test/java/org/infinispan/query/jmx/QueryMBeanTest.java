package org.infinispan.query.jmx;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * // TODO: Document this
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.jmx.QueryMBeanTest")
public class QueryMBeanTest extends SingleCacheManagerTest {

   static final String JMX_DOMAIN = QueryMBeanTest.class.getSimpleName();
   static final String CACHE_NAME = "queryable-cache";
   MBeanServer server;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      EmbeddedCacheManager cm =
            TestCacheManagerFactory.createCacheManagerEnforceJmxDomain(JMX_DOMAIN);

      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.indexing().enable().indexLocalOnly(false)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      cm.defineConfiguration(CACHE_NAME, builder.build());
      return cm;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
   }

   public void testQueryStatsMBean() throws Exception {
      cacheManager.getCache(CACHE_NAME); // Start cache
      ObjectName name = getQueryStatsObjectName(JMX_DOMAIN, CACHE_NAME);
      assert server.isRegistered(name);
      assert !(Boolean) server.getAttribute(name, "StatisticsEnabled");
      server.setAttribute(name, new Attribute("StatisticsEnabled", true));
      assert (Boolean) server.getAttribute(name, "StatisticsEnabled");
   }

   ObjectName getQueryStatsObjectName(String jmxDomain, String cacheName) {
      try {
         return new ObjectName(jmxDomain + ":type=Query,name="
               + ObjectName.quote(cacheName) + ",component=Statistics");
      } catch (MalformedObjectNameException e) {
         throw new CacheException("Malformed object name", e);
      }
   }

}
