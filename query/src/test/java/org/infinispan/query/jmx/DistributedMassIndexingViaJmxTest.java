package org.infinispan.query.jmx;

import static org.infinispan.test.fwk.TestCacheManagerFactory.configureJmx;

import java.net.URL;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.configuration.parsing.ParserRegistry;
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
@Test(groups = "functional", testName = "query.jmx.DistributedMassIndexingViaJmxTest")
public class DistributedMassIndexingViaJmxTest extends DistributedMassIndexingTest {

   private static final String BASE_JMX_DOMAIN = DistributedMassIndexingViaJmxTest.class.getName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; i++) {
         URL url = FileLookupFactory.newInstance().lookupFileLocation(
               "dynamic-indexing-distribution.xml",
               Thread.currentThread().getContextClassLoader());
         ParserRegistry parserRegistry = new ParserRegistry(Thread.currentThread().getContextClassLoader());
         ConfigurationBuilderHolder holder = parserRegistry.parse(url);
         configureJmx(holder.getGlobalConfigurationBuilder(), BASE_JMX_DOMAIN + i, mBeanServerLookup);

         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(holder);
         registerCacheManager(cm);
         cm.getCache();
      }
      waitForClusterToForm();
   }

   @Override
   protected void rebuildIndexes() throws Exception {
      String cacheManagerName = manager(0).getCacheManagerConfiguration().cacheManagerName();
      String defaultCacheName = manager(0).getCacheManagerConfiguration().defaultCacheName().orElse(null);
      ObjectName massIndexerObjName = getMassIndexerObjectName(
            BASE_JMX_DOMAIN + 0, cacheManagerName, defaultCacheName);
      mBeanServerLookup.getMBeanServer().invoke(massIndexerObjName, "start", new Object[0], new String[0]);
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
