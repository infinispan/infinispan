package org.infinispan.query.core.tests;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.core.Search;
import org.infinispan.query.core.stats.IndexStatistics;
import org.infinispan.query.core.stats.QueryStatistics;
import org.infinispan.query.core.stats.SearchStatistics;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.core.tests.QueryCoreTest")
public class QueryCoreTest extends SingleCacheManagerTest {

   private Cache<String, Person> cacheWithStats;

   private static class Person {

      private String _name;

      private String _surname;

      public String getName() {
         return _name;
      }

      public void setName(String name) {
         this._name = name;
      }

      public String getSurname() {
         return _surname;
      }

      public void setSurname(String surname) {
         this._surname = surname;
      }

      @Override
      public String toString() {
         return "Person{name='" + _name + "', surname='" + _surname + "'}";
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(false);
      ConfigurationBuilder stat = getDefaultStandaloneCacheConfig(false);
      stat.statistics().enable();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      cm.defineConfiguration("test", c.build());
      cm.defineConfiguration("stat", stat.build());
      cache = cm.getCache("test");
      cacheWithStats = cm.getCache("stat");
      return cm;
   }

   public void testQuery() {
      Person spidey = new Person();
      spidey.setName("Hombre");
      spidey.setSurname("Araña");
      cache.put("key1", spidey);

      Person superMujer = new Person();
      superMujer.setName("Super");
      superMujer.setSurname("Woman");
      cache.put("key2", superMujer);

      assertEquals(2, cache.size());

      QueryFactory queryFactory = Search.getQueryFactory(cache);

      Query<Person> query = queryFactory.create("from " + Person.class.getName() + " where name='Hombre'");
      List<Person> results = query.execute().list();

      assertEquals("Araña", results.get(0).getSurname());
   }

   @Test
   public void testStats() {
      String q = String.format("FROM %s", Person.class.getName());

      // Cache without stats enabled
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      Query<Person> query = queryFactory.create(q);
      query.execute().list();

      SearchStatistics searchStatistics = Search.getSearchStatistics(cache);
      QueryStatistics queryStatistics = searchStatistics.getQueryStatistics();
      IndexStatistics indexStatistics = searchStatistics.getIndexStatistics();

      assertTrue(indexStatistics.indexInfos().isEmpty());
      assertTrue(await(Search.getClusteredSearchStatistics(cache)).getIndexStatistics().indexInfos().isEmpty());

      assertEquals(0, queryStatistics.getNonIndexedQueryCount());

      // Cache with stats enabled
      queryFactory = Search.getQueryFactory(cacheWithStats);
      query = queryFactory.create(String.format("FROM %s", Person.class.getName()));
      query.execute().list();

      searchStatistics = Search.getSearchStatistics(cacheWithStats);
      queryStatistics = searchStatistics.getQueryStatistics();
      indexStatistics = searchStatistics.getIndexStatistics();

      assertTrue(indexStatistics.indexInfos().isEmpty());
      assertTrue(await(Search.getClusteredSearchStatistics(cacheWithStats)).getIndexStatistics().indexInfos().isEmpty());

      assertEquals(1, queryStatistics.getNonIndexedQueryCount());
      assertTrue(queryStatistics.getNonIndexedQueryAvgTime() > 0);
      assertTrue(queryStatistics.getNonIndexedQueryMaxTime() > 0);
      assertTrue(queryStatistics.getNonIndexedQueryTotalTime() > 0);
      assertEquals(q, queryStatistics.getSlowestNonIndexedQuery());
   }
}
