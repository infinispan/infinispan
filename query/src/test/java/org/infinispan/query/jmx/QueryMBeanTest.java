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
      assert server.isRegistered(name);
      assert !(Boolean) server.getAttribute(name, "StatisticsEnabled");
      server.setAttribute(name, new Attribute("StatisticsEnabled", true));
      assert (Boolean) server.getAttribute(name, "StatisticsEnabled");
   }

   public void testQueryStats() throws Exception {
      cacheManager.getCache(CACHE_NAME); // Start cache
      ObjectName name = getQueryStatsObjectName(JMX_DOMAIN, CACHE_NAME);

      try {
         assert server.isRegistered(name);

         if(!(Boolean) server.getAttribute(name, "StatisticsEnabled")) {
            server.setAttribute(name, new Attribute("StatisticsEnabled", true));
         }

         prepareTestingData();
         Cache cache = cacheManager.getCache(CACHE_NAME);

         QueryParser queryParser = createQueryParser("blurb");
         Query luceneQuery = queryParser.parse("value");
         CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery( luceneQuery );
         List<Object> found = cacheQuery.list();

         assertEquals(numberOfEntries, found.size());
         assertEquals(numberOfEntries, server.invoke(name, "getNumberOfIndexedEntities",
                                       new Object[]{Person.class.getCanonicalName()},
                                       new String[]{String.class.getCanonicalName()}));
         assertEquals(1, server.invoke(name, "getNumberOfIndexedEntities",
                                       new Object[]{AnotherGrassEater.class.getCanonicalName()},
                                       new String[]{String.class.getCanonicalName()}));

         Set<String> classNames = (Set<String>) server.getAttribute(name, "IndexedClassNames");
         assertEquals(2, classNames.size());
         assertTrue("The set should contain the Person class name.", classNames.contains(Person.class.getCanonicalName()));
         assertTrue("The set should contain the AnotherGrassEater class name.", classNames.contains(AnotherGrassEater.class.getCanonicalName()));

//         assertTrue("The query execution total time should be > 0.", (Long) server.getAttribute(name, "SearchQueryTotalTime") > 0);

//         System.out.println(server.getAttribute(name, "SearchQueryTotalTime"));
//         System.out.println(server.getAttribute(name, "SearchQueryExecutionCount"));
//         System.out.println(server.getAttribute(name, "SearchQueryExecutionMaxTimeQueryString"));
//         System.out.println(server.getAttribute(name, "SearchQueryExecutionMaxTime"));
//         System.out.println(server.getAttribute(name, "SearchQueryExecutionCount"));
//         System.out.println(server.getAttribute(name, "SearchQueryExecutionAvgTime"));
//         System.out.println(server.getAttribute(name, "ObjectsLoadedCount"));
//         System.out.println(server.getAttribute(name, "ObjectLoadingTotalTime"));
//         System.out.println(server.getAttribute(name, "ObjectLoadingExecutionMaxTime"));
//         System.out.println(server.getAttribute(name, "ObjectLoadingExecutionAvgTime"));
//         server.invoke(name, "clear",
//                       new Object[] {},
//                       new String[]{});


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

   private void prepareTestingData() {
      Cache cache = cacheManager.getCache(CACHE_NAME);
      for(int i = 0; i < numberOfEntries; i++) {
         Person person = new Person();
         person.setName("key" + i);
         person.setAge(i);
         person.setBlurb("value " + i);
         person.setNonSearchableField("i: " + i);

         cache.put("key" + i, person);
      }

      AnotherGrassEater anotherGrassEater = new AnotherGrassEater("Another grass-eater", "Eats grass");
      cache.put("key101", anotherGrassEater);
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
