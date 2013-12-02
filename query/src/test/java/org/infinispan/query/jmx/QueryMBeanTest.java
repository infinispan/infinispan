package org.infinispan.query.jmx;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.jmx.PerThreadMBeanServerLookup;
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

import javax.management.Attribute;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.List;
import java.util.Set;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

   private int numberOfEntries = 100;

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
      assertTrue(server.isRegistered(name));
      assertFalse((Boolean) server.getAttribute(name, "StatisticsEnabled"));
      server.setAttribute(name, new Attribute("StatisticsEnabled", true));
      assertTrue((Boolean) server.getAttribute(name, "StatisticsEnabled"));
   }

   public void testQueryStats() throws Exception {
      cacheManager.getCache(CACHE_NAME); // Start cache
      ObjectName name = getQueryStatsObjectName(JMX_DOMAIN, CACHE_NAME);

      try {
         assertTrue(server.isRegistered(name));

         if(!(Boolean) server.getAttribute(name, "StatisticsEnabled")) {
            server.setAttribute(name, new Attribute("StatisticsEnabled", true));
         }

         Cache cache = cacheManager.getCache(CACHE_NAME);

         // check that our settings are not ignored
         SearchManager searchManager = Search.getSearchManager(cache);
         assertTrue(searchManager.getSearchFactory().getStatistics().isStatisticsEnabled());

         // add some test data
         for(int i = 0; i < numberOfEntries; i++) {
            Person person = new Person();
            person.setName("key" + i);
            person.setAge(i);
            person.setBlurb("value " + i);
            person.setNonSearchableField("i: " + i);

            cache.put("key" + i, person);
         }

         // after adding more classes and reconfiguring the SearchFactory it might happen isStatisticsEnabled is reset, so we check again
         assertTrue(searchManager.getSearchFactory().getStatistics().isStatisticsEnabled());

         assertEquals(0L, server.getAttribute(name, "SearchQueryExecutionCount"));

         QueryParser queryParser = createQueryParser("blurb");
         Query luceneQuery = queryParser.parse("value");
         CacheQuery cacheQuery = searchManager.getQuery(luceneQuery);
         List<Object> found = cacheQuery.list();

         assertEquals(1L, server.getAttribute(name, "SearchQueryExecutionCount"));

         assertEquals(numberOfEntries, found.size());
         assertEquals(numberOfEntries, server.invoke(name, "getNumberOfIndexedEntities",
                                       new Object[]{Person.class.getCanonicalName()},
                                       new String[]{String.class.getCanonicalName()}));

         assertEquals(1, searchManager.getSearchFactory().getStatistics().indexedEntitiesCount().size());

         // add more test data
         AnotherGrassEater anotherGrassEater = new AnotherGrassEater("Another grass-eater", "Eats grass");
         cache.put("key101", anotherGrassEater);

         cacheQuery = searchManager.getQuery(luceneQuery);
         found = cacheQuery.list();
         assertEquals(numberOfEntries, found.size());

         assertEquals(1, server.invoke(name, "getNumberOfIndexedEntities",
                                       new Object[]{AnotherGrassEater.class.getCanonicalName()},
                                       new String[]{String.class.getCanonicalName()}));

         Set<String> classNames = (Set<String>) server.getAttribute(name, "IndexedClassNames");
         assertEquals(2, classNames.size());
         assertTrue("The set should contain the Person class name.", classNames.contains(Person.class.getCanonicalName()));
         assertTrue("The set should contain the AnotherGrassEater class name.", classNames.contains(AnotherGrassEater.class.getCanonicalName()));
         assertEquals(2, searchManager.getSearchFactory().getStatistics().indexedEntitiesCount().size());

         // check the statistics and see they have reasonable values
         assertTrue("The query execution total time should be > 0.", (Long) server.getAttribute(name, "SearchQueryTotalTime") > 0);
         assertEquals(2L, server.getAttribute(name, "SearchQueryExecutionCount"));
         assertEquals("blurb:value", server.getAttribute(name, "SearchQueryExecutionMaxTimeQueryString"));
         assertTrue((Long) server.getAttribute(name, "SearchQueryExecutionMaxTime") > 0);
         assertTrue((Long) server.getAttribute(name, "SearchQueryExecutionAvgTime") > 0);

         server.invoke(name, "clear",
                       new Object[] {},
                       new String[]{});

         // after "clear" everything must be reset
         assertEquals(0L, server.getAttribute(name, "SearchQueryExecutionCount"));
         assertEquals("", server.getAttribute(name, "SearchQueryExecutionMaxTimeQueryString"));
         assertEquals(0L, server.getAttribute(name, "SearchQueryExecutionMaxTime"));
         assertEquals(0L, server.getAttribute(name, "SearchQueryExecutionAvgTime"));
         assertEquals(0L, server.getAttribute(name, "ObjectsLoadedCount"));
         assertEquals(0L, server.getAttribute(name, "ObjectLoadingTotalTime"));
         assertEquals(0L, server.getAttribute(name, "ObjectLoadingExecutionMaxTime"));
         assertEquals(0L, server.getAttribute(name, "ObjectLoadingExecutionAvgTime"));

      } finally {
         //resetting statistics
         server.setAttribute(name, new Attribute("StatisticsEnabled", false));
      }
   }

   /**
    * Tests that shutting down a cache manager does not interfere with the query related MBeans belonging to a second
    * one that is still alive and shares the same JMX domain (see issue ISPN-3531).
    */
   public void testJmxUnregistration() throws Exception {
      cacheManager.getCache(CACHE_NAME); // Start the cache belonging to first cache manager
      ObjectName queryStatsObjectName = getQueryStatsObjectName(JMX_DOMAIN, CACHE_NAME);
      Set<ObjectName> matchingNames = server.queryNames(new ObjectName(JMX_DOMAIN + ":type=Query,component=Statistics,cache=" + ObjectName.quote(CACHE_NAME) + ",*"), null);
      assertEquals(1, matchingNames.size());
      assertTrue(matchingNames.contains(queryStatsObjectName));

      EmbeddedCacheManager cm2 = null;
      try {
         ConfigurationBuilder defaultCacheConfig2 = new ConfigurationBuilder();
         defaultCacheConfig2
               .indexing().enable()
               .addProperty("default.directory_provider", "ram")
               .addProperty("lucene_version", "LUCENE_CURRENT")
               .jmxStatistics().enable();

         cm2 = TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain("cm2", JMX_DOMAIN, true, true, defaultCacheConfig2, new PerThreadMBeanServerLookup());
         cm2.getCache(CACHE_NAME); // Start the cache belonging to second cache manager

         matchingNames = server.queryNames(new ObjectName(JMX_DOMAIN + ":type=Query,component=Statistics,cache=" + ObjectName.quote(CACHE_NAME) + ",*"), null);
         assertEquals(2, matchingNames.size());
         assertTrue(matchingNames.contains(queryStatsObjectName));
      } finally {
         TestingUtil.killCacheManagers(cm2);
      }

      matchingNames = server.queryNames(new ObjectName(JMX_DOMAIN + ":type=Query,component=Statistics,cache=" + ObjectName.quote(CACHE_NAME) + ",*"), null);
      assertEquals(1, matchingNames.size());
      assertTrue(matchingNames.contains(queryStatsObjectName));
   }

   private ObjectName getQueryStatsObjectName(String jmxDomain, String cacheName) {
      String cacheManagerName = cacheManager.getCacheManagerConfiguration().globalJmxStatistics().cacheManagerName();
      try {
         return new ObjectName(jmxDomain + ":type=Query,manager=" + ObjectName.quote(cacheManagerName)
                                     + ",cache=" + ObjectName.quote(cacheName)
                                     + ",component=Statistics");
      } catch (MalformedObjectNameException e) {
         throw new CacheException("Malformed object name", e);
      }
   }

}
