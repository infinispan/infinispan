package org.infinispan.query.jmx;

import java.io.InputStream;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.commons.util.FileLookup;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.distributed.DistributedMassIndexingTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test reindexing happens when executed via JMX
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = /*functional*/"unstable", testName = "query.jmx.DistributedMassIndexingViaJmxTest", description = "Unstable, see https://issues.jboss.org/browse/ISPN-4012")
public class DistributedMassIndexingViaJmxTest extends DistributedMassIndexingTest {

   static final String BASE_JMX_DOMAIN = DistributedMassIndexingViaJmxTest.class.getSimpleName();
   MBeanServer server;

   @Override
   protected void createCacheManagers() throws Throwable {
      server = PerThreadMBeanServerLookup.getThreadMBeanServer();
      for (int i = 0; i < NUM_NODES; i++) {
         InputStream is = new FileLookup().lookupFileStrict(
               "dynamic-indexing-distribution.xml",
               Thread.currentThread().getContextClassLoader());
         ParserRegistry parserRegistry = new ParserRegistry(
               Thread.currentThread().getContextClassLoader());
         ConfigurationBuilderHolder holder = parserRegistry.parse(is);
         // Each cache manager should use a different jmx domain and
         // a parallel-testsuite friendly mbean server
         holder.getGlobalConfigurationBuilder().globalJmxStatistics()
               .jmxDomain(BASE_JMX_DOMAIN + i)
               .mBeanServerLookup(new PerThreadMBeanServerLookup());

         EmbeddedCacheManager cm = TestCacheManagerFactory
               .createClusteredCacheManager(holder, true);
         registerCacheManager(cm);
         Cache cache = cm.getCache();
         caches.add(cache);
      }
      waitForClusterToForm(neededCacheNames);
   }

   @Override
   protected void rebuildIndexes() throws Exception {
      String cacheManagerName = manager(0).getCacheManagerConfiguration().globalJmxStatistics().cacheManagerName();
      ObjectName massIndexerObjName = getMassIndexerObjectName(
            BASE_JMX_DOMAIN + 0, cacheManagerName, BasicCacheContainer.DEFAULT_CACHE_NAME);
      server.invoke(massIndexerObjName,
            "start", new Object[]{}, new String[]{});
   }

   @Test(groups ="unstable")
   @Override
   public void testReindexing() throws Exception {
      super.testReindexing();
   }

   private ObjectName getMassIndexerObjectName(String jmxDomain, String cacheManagerName, String cacheName) {
      try {
         return new ObjectName(jmxDomain + ":type=Query,manager=" + ObjectName.quote(cacheManagerName)
                                     + ",cache=" + ObjectName.quote(cacheName)
                                     + ",component=MassIndexer");
      } catch (MalformedObjectNameException e) {
         throw new CacheException("Malformed object name", e);
      }
   }

}
