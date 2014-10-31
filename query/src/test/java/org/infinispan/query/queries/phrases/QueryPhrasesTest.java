package org.infinispan.query.queries.phrases;

import org.junit.Assert;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.hibernate.search.exception.SearchException;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.queries.NumericType;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;

/**
 * Tests and verifies that the querying using keywords, phrases, etc works properly.
 *
 * @author Anna Manukyan
 */
@Test(groups = {"functional", "smoke"}, testName = "query.queries.phrases.QueryPhrasesTest")
public class QueryPhrasesTest extends SingleCacheManagerTest {
   private Person person1;
   private Person person2;
   private Person person3;
   private Person person4;

   protected String key1 = "test1";
   protected String key2 = "test2";
   protected String key3 = "test3";

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
            .indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   public void testBooleanQueriesMustNot() throws ParseException {
      loadTestingData();
      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().bool()
            .must(createQueryParser("name").parse("Goat")).not().createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(1, found.size());
      assert found.contains(person1);

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().bool()
            .must(createQueryParser("name").parse("Goat")).createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
   }

   public void testBooleanQueriesOnMultipleTables() throws ParseException {
      loadTestingData();
      AnotherGrassEater anotherGrassEater = new AnotherGrassEater("Another grass-eater", "Eats grass");
      cache.put("key4", anotherGrassEater);

      Query subQuery = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().range()
            .onField("age").below(20).createQuery();


      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(AnotherGrassEater.class).get().bool()
            .should(createQueryParser("name").parse("grass")).should(subQuery).createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(2, found.size());
      assert found.contains(person1);
      assert found.contains(anotherGrassEater);
   }

   public void testBooleanQueriesShould() throws ParseException {
      loadTestingData();
      Query subQuery = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().range()
            .onField("age").below(20).createQuery();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().bool()
            .should(createQueryParser("name").parse("Goat")).should(subQuery).createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(person1);
      assert found.contains(person2);
      assert found.contains(person3);

      subQuery = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().range()
            .onField("age").below(20).excludeLimit().createQuery();

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().bool()
            .should(createQueryParser("name").parse("Goat")).should(subQuery).createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
   }

   public void testBooleanQueriesShouldNot() throws ParseException {
      loadTestingData();

      Query subQuery1 = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get()
            .keyword().onField("name").boostedTo(0.5f)
            .matching("Goat").createQuery();

      Query subQuery2 = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get()
            .range().onField("age").boostedTo(2f).below(20).createQuery();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().bool()
            .should(subQuery1).should(subQuery2).createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.get(0).equals(person1);
      assert found.get(1).equals(person2);
      assert found.get(2).equals(person3);

      subQuery1 = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get()
            .keyword().onField("name").boostedTo(3.5f)
            .matching("Goat").createQuery();

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().bool()
            .should(subQuery1).should(subQuery2).createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.get(0).equals(person2);
      assert found.get(1).equals(person3);
      assert found.get(2).equals(person1);
   }

   public void testFuzzyOnFieldsAndField() {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().keyword().fuzzy().
            onField("name").matching("Goat").createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person3);

      person4 = new Person();
      person4.setName("Test");
      person4.setBlurb("Test goat");
      cache.put("testKey", person4);

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().keyword().fuzzy().
            onField("name").andField("blurb").matching("goat").createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4);

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().keyword().fuzzy().
            onFields("name", "blurb").matching("goat").createQuery();
      List<Object> foundOnFields = Search.getSearchManager(cache).getQuery(query).list();

      AssertJUnit.assertEquals(3, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4);
   }

   public void testFuzzyWithThresholdWithPrefixLength() {
      person1 = new Person("yyJohn", "Eat anything", 10);
      person2 = new Person("yyJonn", "Eat anything", 10);
      cache.put(key1, person1);
      cache.put(key2, person2);

      //Ignore "yy" at the beginning (prefix==2), the difference between the remaining parts of two terms
      //must be no more than edit distance -> return only 1 person
      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().keyword()
            .fuzzy().withEditDistanceUpTo(1).withPrefixLength(2).onField("name").matching("yyJohny").createQuery();
      List<Object> found = Search.getSearchManager(cache).getQuery(query).list();
      AssertJUnit.assertEquals(1, found.size());
      AssertJUnit.assertTrue(found.contains(person1));

      //return all as edit distance excluding the prefix fit all documents
      Query queryReturnAll = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().keyword()
            .fuzzy().withEditDistanceUpTo(2).withPrefixLength(2).onField("name").matching("yyJohn").createQuery();
      List<Object> foundWithLowerThreshold = Search.getSearchManager(cache).getQuery(queryReturnAll).list();
      AssertJUnit.assertEquals(2, foundWithLowerThreshold.size());
      AssertJUnit.assertTrue(foundWithLowerThreshold.contains(person1));
      AssertJUnit.assertTrue(foundWithLowerThreshold.contains(person2));
   }


   public void testQueryingRangeWithAnd() {
      NumericType type1 = new NumericType(10, 20);
      NumericType type2 = new NumericType(20, 10);
      NumericType type3 = new NumericType(10, 10);

      cache.put(key1, type1);
      cache.put(key2, type2);
      cache.put(key3, type3);

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(NumericType.class)
            .get().range().onField("num1").andField("num2").below(20).excludeLimit().createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      AssertJUnit.assertEquals(3, found.size());  //<------ All entries should be here, because andField is executed as SHOULD;
      assert found.contains(type1);
      assert found.contains(type2);
      assert found.contains(type3);

      NumericType type4 = new NumericType(11, 10);
      cache.put("newKey", type4);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(4, found.size());
      assert found.contains(type3);
      assert found.contains(type2);
      assert found.contains(type1);
      assert found.contains(type4);

      //@TODO write here another case with not-matching entries
   }

   @Test(expectedExceptions = SearchException.class)
   public void testWildcardWithWrongName() {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().keyword().wildcard()
            .onField("wrongname").matching("Goat").createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(2, found.size());
   }

   public void testWildcard() {
      loadNumericTypes();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(NumericType.class).get().keyword().wildcard()
            .onField("name").matching("*wildcard*").createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(type1);
      assert found.contains(type2);
      assert found.contains(type3);

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(NumericType.class).get().keyword().wildcard()
            .onField("name").matching("nothing*").createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(0, found.size());

      NumericType type4 = new NumericType(35, 40);
      type4.setName("nothing special.");
      cache.put("otherKey", type4);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(1, found.size());
      assert found.contains(type4);

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(NumericType.class).get().keyword().wildcard()
            .onField("name").matching("*nothing*").createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(2, found.size());
      assert found.contains(type2);
      assert found.contains(type4);
   }

   public void testKeyword() {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().keyword().onField("name")
            .andField("blurb").matching("Eats").createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(2, found.size());

      person4 = new Person();
      person4.setName("Some name with Eats");
      person4.setBlurb("Description without keyword.");

      cache.put("someKey", person4);

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4);
   }

   public void testPhraseSentence() {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().phrase()
            .onField("blurb").sentence("Eats grass").createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(1, found.size());
      assert found.contains(person2);

      person4 = new Person();
      person4.setName("Another goat");
      person4.setBlurb("Eats grass and drinks water.");
      cache.put("anotherKey", person4);

      found = cacheQuery.list();
      Assert.assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person4);
   }

   public void testPhraseSentenceForNonAnalyzedEntries() {
      loadNumericTypes();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(NumericType.class).get().phrase()
            .onField("name").sentence("Some string").createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(0, found.size());

      NumericType type4 = new NumericType(45,50);
      type4.setName("Some string");
      cache.put("otherKey", type4);

      found = cacheQuery.list();
      Assert.assertEquals(1, found.size());
      assert found.contains(type4);
   }

   public void testPhraseWithSlop() {
      loadTestingData();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().phrase().withSlop(3)
            .onField("blurb").sentence("Eats grass").createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(1, found.size());
      assert found.contains(person2);

      person4 = new Person();
      person4.setName("other goat");
      person4.setBlurb("Eats green grass.");
      cache.put("otherKey", person4);

      found = cacheQuery.list();

      Assert.assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person4);

      person4.setBlurb("Eats green tasty grass.");
      cache.put("otherKey", person4);
      found = cacheQuery.list();

      Assert.assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person4);

      person4.setBlurb("Eats green, tasty, juicy grass.");
      cache.put("otherKey", person4);

      found = cacheQuery.list();

      Assert.assertEquals(2, found.size());
      assert found.contains(person2);
      assert found.contains(person4);

      person4.setBlurb("Eats green, tasty, juicy, fresh grass.");
      cache.put("otherKey", person4);

      found = cacheQuery.list();

      Assert.assertEquals(1, found.size());
      assert found.contains(person2);
   }

   public void testPhraseWithSlopWithoutAnalyzer() {
      loadNumericTypes();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(NumericType.class).get().phrase().withSlop(1)
            .onField("name").sentence("Some string").createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(0, found.size());

      NumericType type = new NumericType(10, 60);
      type.setName("Some string");
      cache.put("otherKey", type);

      found = cacheQuery.list();
      Assert.assertEquals(1, found.size());
      assert found.contains(type);

      NumericType type1 = new NumericType(20, 60);
      type1.setName("Some other string");
      cache.put("otherKey1", type1);

      found = cacheQuery.list();
      Assert.assertEquals(1, found.size());
      assert found.contains(type);
   }

   public void testAllExcept() {
      loadTestingData();

      Query subQuery = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().keyword()
            .onField("name").matching("Goat").createQuery();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().all().except().createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(person2);
      assert found.contains(person1);
      assert found.contains(person3);

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().all().except(query).createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(0, found.size());

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(Person.class).get().all().except(subQuery).createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(1, found.size());
      assert found.contains(person1);
   }

   public void testAllExceptWithoutAnalyzer() {
      loadNumericTypes();

      Query subQuery = Search.getSearchManager(cache).buildQueryBuilderForClass(NumericType.class).get().keyword()
            .wildcard().onField("name").matching("*string*").createQuery();

      Query query = Search.getSearchManager(cache).buildQueryBuilderForClass(NumericType.class).get().all().except().createQuery();

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(query);
      List<Object> found = cacheQuery.list();

      Assert.assertEquals(3, found.size());
      assert found.contains(type1);
      assert found.contains(type2);
      assert found.contains(type3);

      query = Search.getSearchManager(cache).buildQueryBuilderForClass(NumericType.class).get().all().except(subQuery).createQuery();

      cacheQuery = Search.getSearchManager(cache).getQuery(query);
      found = cacheQuery.list();

      Assert.assertEquals(0, found.size());
   }

   protected void loadTestingData() {
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
