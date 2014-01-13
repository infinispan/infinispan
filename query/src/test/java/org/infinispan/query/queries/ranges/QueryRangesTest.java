package org.infinispan.query.queries.ranges;

import junit.framework.Assert;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;

/**
 * Tests verifying that query ranges work properly.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.queries.ranges.QueryRangesTest")
public class QueryRangesTest extends SingleCacheManagerTest {
   private Person person1;
   private Person person2;
   private Person person3;
   private Person person4;

   protected String key1 = "test1";
   protected String key2 = "test2";
   protected String key3 = "test3";

   public QueryRangesTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .indexing()
            .enable()
            .indexLocalOnly(false)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testQueryingRangeBelowExcludingLimit() throws ParseException {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class)
            .get().range().onField("age").below(30).excludeLimit().createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      AssertJUnit.assertEquals(2, found.size());
      assert found.contains(person1);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      assert found.size() == 3 : "Size of list should be 3";
      assert found.contains(person1);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRangeBelowWithLimit() throws ParseException {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class)
            .get().range().onField("age").below(30).createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      AssertJUnit.assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      assert found.size() == 4 : "Size of list should be 4";
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRangeAboveExcludingLimit() throws ParseException {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class)
            .get().range().onField("age").above(30).excludeLimit().createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      AssertJUnit.assertEquals(0, found.size());

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class)
            .get().range().onField("age").above(20).excludeLimit().createQuery();
      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      AssertJUnit.assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      assert found.size() == 3 : "Size of list should be 3";
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRangeAboveWithLimit() throws ParseException {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class)
            .get().range().onField("age").above(30).excludeLimit().createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      AssertJUnit.assertEquals(0, found.size());

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class)
            .get().range().onField("age").above(20).createQuery();
      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      AssertJUnit.assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      assert found.size() == 4 : "Size of list should be 3";
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRange() throws ParseException {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class)
            .get().range().onField("age").from(20).excludeLimit().to(30).excludeLimit().createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      AssertJUnit.assertEquals(1, found.size());
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Mighty Goat also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      assert found.size() == 2 : "Size of list should be 3";
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRangeWithLimits() throws ParseException {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class)
            .get().range().onField("age").from(20).to(30).createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      AssertJUnit.assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Mighty Goat also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      assert found.size() == 4 : "Size of list should be 3";
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";

      Person person5 = new Person();
      person5.setName("ANother Goat");
      person5.setBlurb("Some other goat should eat grass.");
      person5.setAge(31);

      cache.put("anotherGoat", person5);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      assert found.size() == 4 : "Size of list should be 3";
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRangeWithLimitsAndExclusions() throws ParseException {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class)
            .get().range().onField("age").from(20).excludeLimit().to(30).createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      AssertJUnit.assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Mighty Goat also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";

      Person person5 = new Person();
      person5.setName("ANother Goat");
      person5.setBlurb("Some other goat should eat grass.");
      person5.setAge(31);

      cache.put("anotherGoat", person5);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class)
            .get().range().onField("age").from(20).to(30).excludeLimit().createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person3);
      assert found.contains(person4);
   }

   public void testQueryingRangeForDatesWithLimitsAndExclusions() throws ParseException {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get()
            .range().onField("dateOfGraduation").from(formatDate("May 5, 2002")).excludeLimit().to(formatDate("June 30, 2012"))
            .createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      AssertJUnit.assertEquals(2, found.size());
      assert found.contains(person1);
      assert found.contains(person2);

      person4 = new Person("Mighty Goat", "Mighty Goat also eats grass", 28, formatDate("June 15, 2007")); //date in ranges
      cache.put("mighty", person4);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person4) : "This should now contain object person4";

      Person person5 = new Person("Another Goat", "Some other goat should eat grass.", 31, formatDate("July 5, 2012")); //date out of ranges
      cache.put("anotherGoat", person5);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person4);

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get()
            .range().onField("dateOfGraduation").from(formatDate("May 5, 2002")).to(formatDate("June 10, 2012")).excludeLimit()
            .createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4);
   }

   protected void loadTestingData() {
      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setBlurb("Likes playing WoW");
      person1.setAge(20);
      person1.setDateOfGraduation(formatDate("June 10, 2012"));

      person2 = new Person();
      person2.setName("Big Goat");
      person2.setBlurb("Eats grass");
      person2.setAge(30);
      person2.setDateOfGraduation(formatDate("July 5, 2002"));

      person3 = new Person();
      person3.setName("Mini Goat");
      person3.setBlurb("Eats cheese");
      person3.setAge(25);
      person3.setDateOfGraduation(formatDate("May 5, 2002"));

      cache.put(key1, person1);
      cache.put(key2, person2);
      cache.put(key3, person3);
   }

   protected Date formatDate(String dateString) {
      Date date = null;
      try {
         date = new SimpleDateFormat("MMMM d, yyyy", Locale.ENGLISH).parse(dateString);
      } catch (java.text.ParseException e) {
         throw new IllegalArgumentException("Unable to parse date.", e);
      }
      return date;
   }
}
