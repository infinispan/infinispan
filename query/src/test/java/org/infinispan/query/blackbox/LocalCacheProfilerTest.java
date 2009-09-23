package org.infinispan.query.blackbox;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.QueryIterator;
import org.infinispan.query.QueryFactory;
import org.infinispan.query.backend.QueryHelper;
import org.infinispan.query.helper.IndexCleanUp;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author Navin Surtani (<a href="mailto:nsurtani@redhat.com">nsurtani@redhat.com</a>)
 */

@Test(groups = "functional", enabled = false)
public class LocalCacheProfilerTest extends SingleCacheManagerTest {
   Person person1;
   Person person2;
   Person person3;
   Person person4;
   Person person5;
   Person person6;
   QueryParser queryParser;
   Query luceneQuery;
   CacheQuery cacheQuery;
   List found;
   String key1 = "Navin";
   String key2 = "BigGoat";
   String key3 = "MiniGoat";

   Cache<String, Person> cache;
   QueryHelper qh;

   protected CacheManager createCacheManager() throws Exception {
      Configuration c = new Configuration();
      c.setTransactionManagerLookupClass(DummyTransactionManagerLookup.class.getName());

      return TestCacheManagerFactory.createCacheManager(c);
   }


   @BeforeMethod
   public void setUp() throws Exception {
      System.setProperty(QueryHelper.QUERY_ENABLED_PROPERTY, "true");
      System.setProperty(QueryHelper.QUERY_INDEX_LOCAL_ONLY_PROPERTY, "false");


      cache = createCacheManager().getCache();

      qh = new QueryHelper(cache, null, Person.class);
      qh.applyProperties();



      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setBlurb("Likes playing WoW");

      person2 = new Person();
      person2.setName("Big Goat");
      person2.setBlurb("Eats grass");

      person3 = new Person();
      person3.setName("Mini Goat");
      person3.setBlurb("Eats cheese");

      person5 = new Person();
      person5.setName("Smelly Cat");
      person5.setBlurb("Eats fish");

      //Put the 3 created objects in the cache.
      cache.put(key1, person1);
      cache.put(key2, person2);
      cache.put(key3, person3);

   }

   @AfterMethod
   public void tearDown() {
      if (cache != null) cache.stop();
      IndexCleanUp.cleanUpIndexes();
   }


   @Test (invocationCount = 2000, enabled = false)
   public void testSimple() throws ParseException {
      cacheQuery = new QueryFactory(cache, qh).getBasicQuery("blurb", "playing");

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);
   }

   @Test (invocationCount = 2000, enabled = false)
   public void testEagerIterator() throws ParseException {

      cacheQuery = new QueryFactory(cache, qh).
            getBasicQuery("blurb", "playing");

      QueryIterator found = cacheQuery.iterator();

      assert found.isFirst();
      assert found.isLast();
   }

   @Test (invocationCount = 2000, enabled = false)
   public void testMultipleResults() throws ParseException {

      queryParser = new QueryParser("name", new StandardAnalyzer());

      luceneQuery = queryParser.parse("goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 2;
      assert found.get(0) == person2;
      assert found.get(1) == person3;

   }

   @Test (invocationCount = 2000, enabled = false)
   public void testModified() throws ParseException {
      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("playing");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);

      person1.setBlurb("Likes pizza");
      cache.put(key1, person1);

      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("pizza");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);
   }

   @Test (invocationCount = 2000, enabled = false)
   public void testAdded() throws ParseException {
      queryParser = new QueryParser("name", new StandardAnalyzer());

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 2 : "Size of list should be 2";
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");

      cache.put("mighty", person4);

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 3 : "Size of list should be 3";
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   @Test (invocationCount = 2000, enabled = false)
   public void testRemoved() throws ParseException {
      queryParser = new QueryParser("name", new StandardAnalyzer());

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 2;
      assert found.contains(person2);
      assert found.contains(person3) : "This should still contain object person3";

      cache.remove(key3);

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.contains(person2);
      assert !found.contains(person3) : "The search should not return person3";


   }

   @Test (invocationCount = 2000, enabled = false)
   public void testSetSort() throws ParseException {
      person2.setAge(35);
      person3.setAge(12);

      Sort sort = new Sort("age");

      queryParser = new QueryParser("name", new StandardAnalyzer());

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 2;

      cacheQuery.setSort(sort);

      found = cacheQuery.list();

      assert found.size() == 2;
      assert found.get(0).equals(person2);
      assert found.get(1).equals(person3);
   }

   @Test (invocationCount = 2000, enabled = false)
   public void testSetFilter() throws ParseException {
      queryParser = new QueryParser("name", new StandardAnalyzer());

      luceneQuery = queryParser.parse("goat");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 2;

      Filter filter = new PrefixFilter(new Term("blurb", "cheese"));

      cacheQuery.setFilter(filter);

      found = cacheQuery.list();

      assert found.size() == 1;

   }

   @Test (invocationCount = 2000, enabled = false)
   public void testLazyIterator() throws ParseException {
      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("playing");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);

      QueryIterator found = cacheQuery.lazyIterator();

      assert found.isFirst();
      assert found.isLast();

   }

   @Test (invocationCount = 2000, enabled = false)
   public void testGetResultSize() throws ParseException {

      queryParser = new QueryParser("blurb", new StandardAnalyzer());
      luceneQuery = queryParser.parse("playing");
      cacheQuery = new QueryFactory(cache, qh).getQuery(luceneQuery);

      assert cacheQuery.getResultSize() == 1;
   }

}
