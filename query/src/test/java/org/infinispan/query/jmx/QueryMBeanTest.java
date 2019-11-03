package org.infinispan.query.jmx;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;
import static org.infinispan.test.fwk.TestCacheManagerFactory.configureGlobalJmx;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;
import java.util.Set;

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test search statistics MBean.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "query.jmx.QueryMBeanTest")
public class QueryMBeanTest extends SingleCacheManagerTest {

   private static final String TEST_JMX_DOMAIN = QueryMBeanTest.class.getSimpleName();

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   private static final String CACHE_NAME = "queryable-cache";

   private static final int numberOfEntries = 100;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder().nonClusteredDefault();
      globalConfiguration.globalJmxStatistics().enable()
            .jmxDomain(TEST_JMX_DOMAIN)
            .mBeanServerLookup(mBeanServerLookup);

      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(true);
      builder.jmxStatistics().enabled(true)
            .indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(globalConfiguration, builder);

      cm.defineConfiguration(CACHE_NAME, builder.build());
      return cm;
   }

   public void testQueryStatsMBean() throws Exception {
      cacheManager.getCache(CACHE_NAME); // Start cache
      ObjectName name = getQueryStatsObjectName(TEST_JMX_DOMAIN, CACHE_NAME);
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      assertTrue(mBeanServer.isRegistered(name));
      assertFalse((Boolean) mBeanServer.getAttribute(name, "StatisticsEnabled"));
      mBeanServer.setAttribute(name, new Attribute("StatisticsEnabled", true));
      assertTrue((Boolean) mBeanServer.getAttribute(name, "StatisticsEnabled"));
   }

   public void testQueryStats() throws Exception {
      cacheManager.getCache(CACHE_NAME); // Start cache
      ObjectName name = getQueryStatsObjectName(TEST_JMX_DOMAIN, CACHE_NAME);

      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      try {
         assertTrue(mBeanServer.isRegistered(name));

         if (!(Boolean) mBeanServer.getAttribute(name, "StatisticsEnabled")) {
            mBeanServer.setAttribute(name, new Attribute("StatisticsEnabled", true));
         }

         Cache<String, Object> cache = cacheManager.getCache(CACHE_NAME);

         // check that our settings are not ignored
         SearchManager searchManager = Search.getSearchManager(cache);
         assertTrue(searchManager.getStatistics().isStatisticsEnabled());

         // add some test data
         for (int i = 0; i < numberOfEntries; i++) {
            Person person = new Person();
            person.setName("key" + i);
            person.setAge(i);
            person.setBlurb("value " + i);
            person.setNonIndexedField("i: " + i);

            cache.put("key" + i, person);
         }

         // after adding more classes and reconfiguring the SearchFactory it might happen isStatisticsEnabled is reset, so we check again
         assertTrue(searchManager.getStatistics().isStatisticsEnabled());

         assertEquals(0L, mBeanServer.getAttribute(name, "SearchQueryExecutionCount"));

         QueryParser queryParser = createQueryParser("blurb");
         Query luceneQuery = queryParser.parse("value");
         CacheQuery<?> cacheQuery = searchManager.getQuery(luceneQuery);
         List<?> found = cacheQuery.list(); //Executing first query

         assertEquals(1L, mBeanServer.getAttribute(name, "SearchQueryExecutionCount"));

         assertEquals(numberOfEntries, found.size());
         assertEquals(numberOfEntries, mBeanServer.invoke(name, "getNumberOfIndexedEntities",
               new Object[]{Person.class.getName()},
               new String[]{String.class.getName()}));

         assertEquals(1, searchManager.getStatistics().indexedEntitiesCount().size());

         // add more test data
         AnotherGrassEater anotherGrassEater = new AnotherGrassEater("Another grass-eater", "Eats grass");
         cache.put("key101", anotherGrassEater);

         cacheQuery = searchManager.getQuery(luceneQuery);
         found = cacheQuery.list(); //Executing second query
         assertEquals(numberOfEntries, found.size());

         assertEquals(1, mBeanServer.invoke(name, "getNumberOfIndexedEntities",
               new Object[]{AnotherGrassEater.class.getName()},
               new String[]{String.class.getName()}));

         Set<String> classNames = (Set<String>) mBeanServer.getAttribute(name, "IndexedClassNames");
         assertEquals(2, classNames.size());
         assertTrue("The set should contain the Person class name.", classNames.contains(Person.class.getName()));
         assertTrue("The set should contain the AnotherGrassEater class name.", classNames.contains(AnotherGrassEater.class.getName()));
         assertEquals(2, searchManager.getStatistics().indexedEntitiesCount().size());

         // check the statistics and see they have reasonable values
         assertTrue("The query execution total time should be > 0.", (Long) mBeanServer.getAttribute(name, "SearchQueryTotalTime") > 0);
         assertEquals(2L, mBeanServer.getAttribute(name, "SearchQueryExecutionCount"));
         assertEquals("blurb:value", mBeanServer.getAttribute(name, "SearchQueryExecutionMaxTimeQueryString"));
         assertTrue((Long) mBeanServer.getAttribute(name, "SearchQueryExecutionMaxTime") > 0);
         assertTrue((Long) mBeanServer.getAttribute(name, "SearchQueryExecutionAvgTime") > 0);

         mBeanServer.invoke(name, "clear", new Object[0], new String[0]);

         // after "clear" everything must be reset
         assertEquals(0L, mBeanServer.getAttribute(name, "SearchQueryExecutionCount"));
         assertEquals("", mBeanServer.getAttribute(name, "SearchQueryExecutionMaxTimeQueryString"));
         assertEquals(0L, mBeanServer.getAttribute(name, "SearchQueryExecutionMaxTime"));
         assertEquals(0L, mBeanServer.getAttribute(name, "SearchQueryExecutionAvgTime"));
         assertEquals(0L, mBeanServer.getAttribute(name, "ObjectsLoadedCount"));
         assertEquals(0L, mBeanServer.getAttribute(name, "ObjectLoadingTotalTime"));
         assertEquals(0L, mBeanServer.getAttribute(name, "ObjectLoadingExecutionMaxTime"));
         assertEquals(0L, mBeanServer.getAttribute(name, "ObjectLoadingExecutionAvgTime"));

      } finally {
         //resetting statistics
         mBeanServer.setAttribute(name, new Attribute("StatisticsEnabled", false));
      }
   }

   /**
    * Tests that shutting down a cache manager does not interfere with the query related MBeans belonging to a second
    * one that is still alive and shares the same JMX domain (see issue ISPN-3531).
    */
   public void testJmxUnregistration() throws Exception {
      cacheManager.getCache(CACHE_NAME); // Start the cache belonging to first cache manager
      ObjectName queryStatsObjectName = getQueryStatsObjectName(TEST_JMX_DOMAIN, CACHE_NAME);
      MBeanServer mBeanServer = mBeanServerLookup.getMBeanServer();
      Set<ObjectName> matchingNames = mBeanServer.queryNames(new ObjectName(TEST_JMX_DOMAIN + ":type=Query,component=Statistics,cache=" + ObjectName.quote(CACHE_NAME) + ",*"), null);
      assertEquals(1, matchingNames.size());
      assertTrue(matchingNames.contains(queryStatsObjectName));

      EmbeddedCacheManager cm2 = null;
      try {
         GlobalConfigurationBuilder globalConfig2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
         globalConfig2.cacheManagerName("cm2");
         configureGlobalJmx(globalConfig2, TEST_JMX_DOMAIN, mBeanServerLookup);
         ConfigurationBuilder defaultCacheConfig2 = new ConfigurationBuilder();
         defaultCacheConfig2
               .indexing().index(Index.ALL)
               .addProperty("default.directory_provider", "local-heap")
               .addProperty("lucene_version", "LUCENE_CURRENT")
               .jmxStatistics().enable();

         cm2 = TestCacheManagerFactory.createClusteredCacheManager(globalConfig2, defaultCacheConfig2);
         cm2.defineConfiguration(CACHE_NAME, defaultCacheConfig2.build());
         cm2.getCache(CACHE_NAME); // Start the cache belonging to second cache manager

         matchingNames = mBeanServer.queryNames(new ObjectName(TEST_JMX_DOMAIN + ":type=Query,component=Statistics,cache=" + ObjectName.quote(CACHE_NAME) + ",*"), null);
         assertEquals(2, matchingNames.size());
         assertTrue(matchingNames.contains(queryStatsObjectName));
      } finally {
         TestingUtil.killCacheManagers(cm2);
      }

      matchingNames = mBeanServer.queryNames(new ObjectName(TEST_JMX_DOMAIN + ":type=Query,component=Statistics,cache=" + ObjectName.quote(CACHE_NAME) + ",*"), null);
      assertEquals(1, matchingNames.size());
      assertTrue(matchingNames.contains(queryStatsObjectName));
   }

   private ObjectName getQueryStatsObjectName(String jmxDomain, String cacheName) {
      String cacheManagerName = cacheManager.getCacheManagerConfiguration().cacheManagerName();
      try {
         return new ObjectName(jmxDomain + ":type=Query,manager=" + ObjectName.quote(cacheManagerName)
               + ",cache=" + ObjectName.quote(cacheName)
               + ",component=Statistics");
      } catch (MalformedObjectNameException e) {
         throw new CacheException("Malformed object name", e);
      }
   }
}
