package org.infinispan.query.queries.phrases;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.List;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.objectfilter.ParsingException;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.queries.NumericType;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Tests and verifies that the querying using keywords, phrases, etc works properly.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.queries.phrases.QueryPhrasesTest")
public class QueryPhrasesTest extends SingleCacheManagerTest {

   private Person person1;
   private Person person2;
   private Person person3;
   private Person person4;

   private String key1 = "test1";
   private String key2 = "test2";
   private String key3 = "test3";

   private NumericType type1;
   private NumericType type2;
   private NumericType type3;

   public QueryPhrasesTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .indexing().enable()
            .addIndexedEntity(NumericType.class)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   private <T> CacheQuery<T> createCacheQuery(Class<T> clazz, String predicate) {
      String queryStr = String.format("FROM %s WHERE %s", clazz.getName(), predicate);
      return Search.getSearchManager(cache).getQuery(queryStr);
   }

   public void testBooleanQueriesMustNot() {
      loadTestingData();

      CacheQuery<Person> cacheQuery = createCacheQuery(Person.class, "-name:'Goat'");
      List<Person> found = cacheQuery.list();

      assertEquals(1, found.size());
      assertTrue(found.contains(person1));

      cacheQuery = createCacheQuery(Person.class, "name:'Goat'");
      found = cacheQuery.list();

      assertEquals(2, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));
   }

   public void testBooleanQueriesShould() {
      loadTestingData();

      CacheQuery<Person> cacheQuery = createCacheQuery(Person.class, "name:'Goat' OR age <= 20");
      List<Person> found = cacheQuery.list();

      assertEquals(3, found.size());
      assertTrue(found.contains(person1));
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));

      cacheQuery = createCacheQuery(Person.class, "name:'Goat' OR age < 20");
      found = cacheQuery.list();

      assertEquals(2, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));
   }

   public void testBooleanQueriesShouldNot() {
      loadTestingData();

      CacheQuery<Person> cacheQuery = createCacheQuery(Person.class, "name:'Goat'^0.5 OR age:[* to 20]^2");
      List<Person> found = cacheQuery.list();

      assertEquals(3, found.size());
      assertEquals(person1, found.get(0));
      assertEquals(person2, found.get(1));
      assertEquals(person3, found.get(2));

      cacheQuery = createCacheQuery(Person.class, "name:'Goat'^3.5 OR age:[* to 20]^2");
      found = cacheQuery.list();

      assertEquals(3, found.size());
      assertEquals(person2, found.get(0));
      assertEquals(person3, found.get(1));
      assertEquals(person1, found.get(2));
   }

   public void testFuzzyOnFieldsAndField() {
      loadTestingData();

      CacheQuery<Person> cacheQuery = createCacheQuery(Person.class, "name:'Goat'~2");
      List<Person> found = cacheQuery.list();

      assertEquals(2, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));

      person4 = new Person();
      person4.setName("Test");
      person4.setBlurb("Test goat");
      cache.put("testKey", person4);

      cacheQuery = createCacheQuery(Person.class, "name:'goat'~2 OR blurb:'goat'~2");
      found = cacheQuery.list();

      assertEquals(3, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));
      assertTrue(found.contains(person4));
   }

   public void testFuzzyWithThresholdWithPrefixLength() {
      person1 = new Person("yyJohn", "Eat anything", 10);
      person2 = new Person("yyJonn", "Eat anything", 10);
      cache.put(key1, person1);
      cache.put(key2, person2);

      //Ignore "yy" at the beginning (prefix==2), the difference between the remaining parts of two terms
      //must be no more than edit distance -> return only 1 person
      CacheQuery<Person> cacheQuery = createCacheQuery(Person.class, "name:'yyJohny'~1");
      List<Person> found = cacheQuery.list();
      assertEquals(1, found.size());
      assertTrue(found.contains(person1));

      //return all as edit distance excluding the prefix fit all documents
      cacheQuery = createCacheQuery(Person.class, "name:'yyJohn'~2");
      List<Person> foundWithLowerThreshold = cacheQuery.list();
      assertEquals(2, foundWithLowerThreshold.size());
      assertTrue(foundWithLowerThreshold.contains(person1));
      assertTrue(foundWithLowerThreshold.contains(person2));
   }

   public void testQueryingRangeWithAnd() {
      NumericType type1 = new NumericType(10, 20);
      NumericType type2 = new NumericType(20, 10);
      NumericType type3 = new NumericType(10, 10);

      cache.put(key1, type1);
      cache.put(key2, type2);
      cache.put(key3, type3);

      CacheQuery<NumericType> cacheQuery = createCacheQuery(NumericType.class, "num1:[* TO 19] OR num2:[* TO 19]");
      List<NumericType> found = cacheQuery.list();

      assertEquals(3, found.size());  //<------ All entries should be here, because andField is executed as SHOULD;
      assertTrue(found.contains(type1));
      assertTrue(found.contains(type2));
      assertTrue(found.contains(type3));

      NumericType type4 = new NumericType(11, 10);
      cache.put("newKey", type4);

      found = cacheQuery.list();

      assertEquals(4, found.size());
      assertTrue(found.contains(type3));
      assertTrue(found.contains(type2));
      assertTrue(found.contains(type1));
      assertTrue(found.contains(type4));

      //@TODO write here another case with not-matching entries
   }

   @Test(expectedExceptions = ParsingException.class)
   public void testWildcardWithWrongName() {
      loadTestingData();

      CacheQuery<Person> cacheQuery = createCacheQuery(Person.class, "wrongname:'*Goat*'");
      List<Person> found = cacheQuery.list();

      assertEquals(2, found.size());
   }

   public void testWildcard() {
      loadNumericTypes();

      CacheQuery<NumericType> cacheQuery = createCacheQuery(NumericType.class, "name LIKE '%wildcard%'");
      List<NumericType> found = cacheQuery.list();

      assertEquals(3, found.size());
      assertTrue(found.contains(type1));
      assertTrue(found.contains(type2));
      assertTrue(found.contains(type3));

      cacheQuery = createCacheQuery(NumericType.class, "name LIKE 'nothing%'");
      found = cacheQuery.list();

      assertEquals(0, found.size());

      NumericType type4 = new NumericType(35, 40);
      type4.setName("nothing special.");
      cache.put("otherKey", type4);

      found = cacheQuery.list();

      assertEquals(1, found.size());
      assertTrue(found.contains(type4));

      cacheQuery = createCacheQuery(NumericType.class, "name LIKE '%nothing%'");
      found = cacheQuery.list();

      assertEquals(2, found.size());
      assertTrue(found.contains(type2));
      assertTrue(found.contains(type4));
   }

   public void testKeyword() {
      loadTestingData();

      CacheQuery<Person> cacheQuery = createCacheQuery(Person.class, "name:'Eats' OR blurb:'Eats'");
      List<Person> found = cacheQuery.list();

      assertEquals(2, found.size());

      person4 = new Person();
      person4.setName("Some name with Eats");
      person4.setBlurb("Description without keyword.");

      cache.put("someKey", person4);

      found = cacheQuery.list();

      assertEquals(3, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));
      assertTrue(found.contains(person4));
   }

   public void testPhraseSentence() {
      loadTestingData();

      CacheQuery<Person> cacheQuery = createCacheQuery(Person.class, "blurb:'Eats grass'");
      List<Person> found = cacheQuery.list();

      assertEquals(1, found.size());
      assertTrue(found.contains(person2));

      person4 = new Person();
      person4.setName("Another goat");
      person4.setBlurb("Eats grass and drinks water.");
      cache.put("anotherKey", person4);

      found = cacheQuery.list();
      assertEquals(2, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person4));
   }

   public void testPhraseSentenceForNonAnalyzedEntries() {
      loadNumericTypes();

      CacheQuery<NumericType> cacheQuery = createCacheQuery(NumericType.class, "name = 'Some string'");
      List<NumericType> found = cacheQuery.list();

      assertEquals(0, found.size());

      NumericType type4 = new NumericType(45, 50);
      type4.setName("Some string");
      cache.put("otherKey", type4);

      found = cacheQuery.list();
      assertEquals(1, found.size());
      assertTrue(found.contains(type4));
   }

   public void testPhraseWithSlop() {
      loadTestingData();

      CacheQuery<Person> cacheQuery = createCacheQuery(Person.class, "blurb:'Eats grass'~3");
      List<Person> found = cacheQuery.list();

      assertEquals(1, found.size());
      assertTrue(found.contains(person2));

      person4 = new Person();
      person4.setName("other goat");
      person4.setBlurb("Eats green grass.");
      cache.put("otherKey", person4);

      found = cacheQuery.list();

      assertEquals(2, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person4));

      person4.setBlurb("Eats green tasty grass.");
      cache.put("otherKey", person4);
      found = cacheQuery.list();

      assertEquals(2, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person4));

      person4.setBlurb("Eats green, tasty, juicy grass.");
      cache.put("otherKey", person4);

      found = cacheQuery.list();

      assertEquals(2, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person4));

      person4.setBlurb("Eats green, tasty, juicy, fresh grass.");
      cache.put("otherKey", person4);

      found = cacheQuery.list();

      assertEquals(1, found.size());
      assertTrue(found.contains(person2));
   }

   public void testPhraseWithSlopWithoutAnalyzer() {
      loadNumericTypes();

      CacheQuery<NumericType> cacheQuery = createCacheQuery(NumericType.class, "name='Some string'");
      List<NumericType> found = cacheQuery.list();

      assertEquals(0, found.size());

      NumericType type = new NumericType(10, 60);
      type.setName("Some string");
      cache.put("otherKey", type);

      found = cacheQuery.list();
      assertEquals(1, found.size());
      assertTrue(found.contains(type));

      NumericType type1 = new NumericType(20, 60);
      type1.setName("Some other string");
      cache.put("otherKey1", type1);

      found = cacheQuery.list();
      assertEquals(1, found.size());
      assertTrue(found.contains(type));
   }

   public void testAllExcept() {
      loadTestingData();

      CacheQuery<Person> cacheQuery = createCacheQuery(Person.class, "name:[* TO *]");
      List<Person> found = cacheQuery.list();

      assertEquals(3, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person1));
      assertTrue(found.contains(person3));

      cacheQuery = createCacheQuery(Person.class, "-name:[* TO *]");
      found = cacheQuery.list();

      assertEquals(0, found.size());

      cacheQuery = createCacheQuery(Person.class, "-name:'Goat'");
      found = cacheQuery.list();

      assertEquals(1, found.size());
      assertTrue(found.contains(person1));
   }

   public void testAllExceptWithoutAnalyzer() {
      loadNumericTypes();

      CacheQuery<NumericType> cacheQuery = createCacheQuery(NumericType.class, "name LIKE '%string%'");
      List<NumericType> found = cacheQuery.list();

      assertEquals(3, found.size());
      assertTrue(found.contains(type1));
      assertTrue(found.contains(type2));
      assertTrue(found.contains(type3));

      cacheQuery = createCacheQuery(NumericType.class, "not(name LIKE '%string%')");
      found = cacheQuery.list();

      assertEquals(0, found.size());
   }

   private void loadTestingData() {
      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setBlurb("Likes playing WoW");
      person1.setAge(20);

      person2 = new Person();
      person2.setName("Big Goat");
      person2.setBlurb("Eats grass");
      person2.setAge(30);

      person3 = new Person();
      person3.setName("Mini Goat");
      person3.setBlurb("Eats cheese");
      person3.setAge(25);

      cache.put(key1, person1);
      cache.put(key2, person2);
      cache.put(key3, person3);
   }

   private void loadNumericTypes() {
      type1 = new NumericType(10, 20);
      type1.setName("Some string for testing wildcards.");

      type2 = new NumericType(15, 25);
      type2.setName("This string has nothing to do with wildcards.");

      type3 = new NumericType(20, 30);
      type3.setName("Some other string for testing wildcards.");

      cache.put(key1, type1);
      cache.put(key2, type2);
      cache.put(key3, type3);
   }
}
