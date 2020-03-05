package org.infinispan.query.queries.ranges;

import static org.testng.AssertJUnit.assertEquals;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests verifying that query ranges work properly.
 *
 * @author Anna Manukyan
 */
@Test(groups = {"functional"}, testName = "query.queries.ranges.QueryRangesTest")
public class QueryRangesTest extends SingleCacheManagerTest {

   protected final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

   private Person person1;
   private Person person2;
   private Person person3;
   private Person person4;

   protected String key1 = "test1";
   protected String key2 = "test2";
   protected String key3 = "test3";

   public QueryRangesTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
      DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("GMT"));
   }

   protected CacheQuery<Person> createQuery(String predicate) {
      SearchManager searchManager = Search.getSearchManager(cache);
      String query = String.format("FROM %s WHERE %s", Person.class.getName(), predicate);
      return searchManager.getQuery(query);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .indexing()
            .enable()
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testQueryingRangeBelowExcludingLimit() throws ParseException {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("age:[* TO 29]");
      List<?> found = cacheQuery.list();

      assertEquals(2, found.size());
      assert found.contains(person1);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      found = cacheQuery.list();

      assert found.size() == 3 : "Size of list should be 3";
      assert found.contains(person1);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRangeBelowWithLimit() throws ParseException {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("age:[* to 30]");
      List<?> found = cacheQuery.list();

      assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      found = cacheQuery.list();

      assert found.size() == 4 : "Size of list should be 4";
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRangeAboveExcludingLimit() throws ParseException {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("age:[31 to *]");
      List<?> found = cacheQuery.list();

      assertEquals(0, found.size());

      cacheQuery = createQuery("age:[21 to *]");
      found = cacheQuery.list();

      assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      found = cacheQuery.list();

      assert found.size() == 3 : "Size of list should be 3";
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRangeAboveWithLimit() throws ParseException {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("age:[31 to *]");
      List<?> found = cacheQuery.list();

      assertEquals(0, found.size());

      cacheQuery = createQuery("age:[20 to *]");
      found = cacheQuery.list();

      assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      found = cacheQuery.list();

      assert found.size() == 4 : "Size of list should be 3";
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRange() throws ParseException {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("age:[21 TO 29]");
      List<?> found = cacheQuery.list();

      assertEquals(1, found.size());
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Mighty Goat also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      found = cacheQuery.list();

      assert found.size() == 2 : "Size of list should be 3";
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRangeWithLimits() throws ParseException {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("age:[20 to 30]");
      List<?> found = cacheQuery.list();

      assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Mighty Goat also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

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

      found = cacheQuery.list();

      assert found.size() == 4 : "Size of list should be 3";
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testQueryingRangeWithLimitsAndExclusions() throws ParseException {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("age:[21 to 30]");
      List<?> found = cacheQuery.list();

      assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Mighty Goat also eats grass");
      person4.setAge(28);

      cache.put("mighty", person4);

      found = cacheQuery.list();

      assertEquals(3, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";

      Person person5 = new Person();
      person5.setName("ANother Goat");
      person5.setBlurb("Some other goat should eat grass.");
      person5.setAge(31);

      cache.put("anotherGoat", person5);

      found = cacheQuery.list();

      assertEquals(3, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";

      cacheQuery = createQuery("age:[20 to 29]");
      found = cacheQuery.list();

      assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person3);
      assert found.contains(person4);
   }

   public void testQueryingRangeForDatesWithLimitsAndExclusions() throws ParseException {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("dateOfGraduation:['20020506' to '20120630']");
      List<?> found = cacheQuery.list();

      assertEquals(2, found.size());
      assert found.contains(person1);
      assert found.contains(person2);

      person4 = new Person("Mighty Goat", "Mighty Goat also eats grass", 28, makeDate("2007-06-15")); //date in ranges
      cache.put("mighty", person4);

      found = cacheQuery.list();

      assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person4) : "This should now contain object person4";

      Person person5 = new Person("Another Goat", "Some other goat should eat grass.", 31, makeDate("2012-07-05")); //date out of ranges
      cache.put("anotherGoat", person5);

      found = cacheQuery.list();

      assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person4);

      cacheQuery = createQuery("dateOfGraduation:['20020505' to '20120609']");
      found = cacheQuery.list();
      assertEquals(3, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4);
   }

   protected void loadTestingData() throws ParseException {
      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setBlurb("Likes playing WoW");
      person1.setAge(20);
      person1.setDateOfGraduation(makeDate("2012-06-10"));

      person2 = new Person();
      person2.setName("Big Goat");
      person2.setBlurb("Eats grass");
      person2.setAge(30);
      person2.setDateOfGraduation(makeDate("2002-07-05"));

      person3 = new Person();
      person3.setName("Mini Goat");
      person3.setBlurb("Eats cheese");
      person3.setAge(25);
      person3.setDateOfGraduation(makeDate("2002-05-05"));

      cache.put(key1, person1);
      cache.put(key2, person2);
      cache.put(key3, person3);
   }

   protected Date makeDate(String dateStr) throws ParseException {
      return DATE_FORMAT.parse(dateStr);
   }
}
