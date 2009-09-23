package org.infinispan.query.blackbox;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.QueryFactory;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.backend.QueryHelper;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author Navin Surtani
 */
@Test(groups = "functional")
public class ClusteredCacheTest extends MultipleCacheManagersTest {
   Cache<String, Person> cache1, cache2;
   Person person1;
   Person person2;
   Person person3;
   Person person4;
   QueryParser queryParser;
   Query luceneQuery;
   CacheQuery cacheQuery;
   QueryHelper qh;
   List found;
   String key1 = "Navin";
   String key2 = "BigGoat";
   String key3 = "MiniGoat";
   private static final Log log = LogFactory.getLog(Person.class);

   public ClusteredCacheTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void createCacheManagers() throws Throwable {

      Configuration cacheCfg = new Configuration();
      cacheCfg.setCacheMode(Configuration.CacheMode.REPL_SYNC);
      cacheCfg.setFetchInMemoryState(false);

      List<Cache<String, Person>> caches = createClusteredCaches(2, "infinispan-query", cacheCfg);

      cache1 = caches.get(0);
      cache2 = caches.get(1);

      // We will put objects into cache1 and then try and run the queries on cache2. This would mean that indexLocal
      // must be set to false.

      System.setProperty(QueryHelper.QUERY_ENABLED_PROPERTY, "true");
      System.setProperty(QueryHelper.QUERY_INDEX_LOCAL_ONLY_PROPERTY, "false");

      qh = TestQueryHelperFactory.createTestQueryHelperInstance(cache2, Person.class);

      TestingUtil.blockUntilViewsReceived(60000, cache1, cache2);

      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setBlurb("Likes playing WoW");

      person2 = new Person();
      person2.setName("BigGoat");
      person2.setBlurb("Eats grass");

      person3 = new Person();
      person3.setName("MiniGoat");
      person3.setBlurb("Eats cheese");

      //Put the 3 created objects in the cache1.

      cache1.put(key1, person1);
      cache1.put(key2, person2);
      cache1.put(key3, person3);

   }

   public void testSimple() throws ParseException {
      cacheQuery = new QueryFactory(cache2, qh)
            .getBasicQuery("blurb", "playing");

      found = cacheQuery.list();

      assert found.size() == 1;

      if (found.get(0) == null) {
         log.warn("found.get(0) is null");
         Person p1 = cache2.get(key1);
         if (p1 == null) {
            log.warn("Person p1 is null in sc2 and cannot actually see the data of person1 in sc1");
         } else {
            log.trace("p1 name is  " + p1.getName());

         }
      }

      assert found.get(0).equals(person1);

   }

   public void testModified() throws ParseException {
      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("playing");
      cacheQuery = new QueryFactory(cache2, qh).getQuery(luceneQuery);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);

      person1.setBlurb("Likes pizza");
      cache1.put("Navin", person1);


      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("pizza");
      cacheQuery = new QueryFactory(cache2, qh).getQuery(luceneQuery);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);
   }

   public void testAdded() throws ParseException {
      queryParser = new QueryParser("blurb", new StandardAnalyzer());

      luceneQuery = queryParser.parse("eats");
      cacheQuery = new QueryFactory(cache2, qh).getQuery(luceneQuery);
      found = cacheQuery.list();


      assert found.size() == 2 : "Size of list should be 2";
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");

      cache1.put("mighty", person4);

      luceneQuery = queryParser.parse("eats");
      cacheQuery = new QueryFactory(cache2, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 3 : "Size of list should be 3";
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testRemoved() throws ParseException {
      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("eats");
      cacheQuery = new QueryFactory(cache2, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 2;
      assert found.contains(person2);
      assert found.contains(person3) : "This should still contain object person3";

      cache1.remove(key3);

      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("eats");
      cacheQuery = new QueryFactory(cache2, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

   }

   public void testGetResultSize() throws ParseException {

      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("playing");
      cacheQuery = new QueryFactory(cache2, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 1;

   }


}


