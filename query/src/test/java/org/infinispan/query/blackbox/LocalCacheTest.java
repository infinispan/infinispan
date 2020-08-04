package org.infinispan.query.blackbox;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.infinispan.Cache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "query.blackbox.LocalCacheTest")
public class LocalCacheTest extends SingleCacheManagerTest {
   protected Person person1;
   protected Person person2;
   protected Person person3;
   protected Person person4;
   protected AnotherGrassEater anotherGrassEater;
   protected String key1 = "Navin";
   protected String key2 = "BigGoat";
   protected String key3 = "MiniGoat";
   protected String anotherGrassEaterKey = "anotherGrassEaterKey";

   public LocalCacheTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   private <T> Query<T> createQuery(String predicate, Class<T> entity) {
      QueryFactory queryFactory = Search.getQueryFactory(cache);
      return queryFactory.create(String.format("FROM %s WHERE %s", entity.getName(), predicate));
   }

   public void testSimple() {
      loadTestingData();
      Query<Person> cacheQuery = createQuery("blurb:'playing'", Person.class);

      List<Person> found = cacheQuery.execute().list();

      int elems = found.size();
      assert elems == 1 : "Expected 1 but was " + elems;

      Person val = found.get(0);
      assert val.equals(person1) : "Expected " + person1 + " but was " + val;
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testEagerIterator() {
      loadTestingData();
      Query<Person> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (CloseableIterator<Person> found = cacheQuery.iterator()) {
         assertTrue(found.hasNext());
         found.next();
         assertFalse(found.hasNext());
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testIteratorWithProjections() {
      loadTestingData();

      QueryFactory queryFactory = Search.getQueryFactory(cache);
      String q = String.format("SELECT name, blurb from %s p where name:'navin'", Person.class.getName());
      Query<Object[]> query = queryFactory.create(q);

      try (CloseableIterator<Object[]> found = query.iterator()) {
         assertTrue(found.hasNext());
         Object[] next = found.next();
         assertEquals("Navin Surtani", next[0]);
         assertEquals("Likes playing WoW", next[1]);
         assertFalse(found.hasNext());
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testEagerIteratorRemove() {
      loadTestingData();
      Query<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (CloseableIterator<?> found = cacheQuery.iterator()) {
         assertTrue(found.hasNext());
         found.remove();
      }
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testEagerIteratorExCase() {
      loadTestingData();
      Query<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (CloseableIterator<?> found = cacheQuery.iterator()) {
         assertTrue(found.hasNext());
         found.next();
         assertFalse(found.hasNext());
         found.next();
      }
   }

   public void testMultipleResults() {
      loadTestingData();

      Query<Person> cacheQuery = createQuery("name:'goat'", Person.class);
      QueryResult<Person> result = cacheQuery.execute();
      List<Person> found = result.list();

      assert found.size() == 2;
      assertEquals(2, getNumberOfHits(result));
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testModified() {
      loadTestingData();
      Query<Person> cacheQuery = createQuery("blurb:'playing'", Person.class);

      List<Person> found = cacheQuery.execute().list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);

      person1.setBlurb("Likes pizza");
      cache.put(key1, person1);

      cacheQuery = createQuery("blurb:'pizza'", Person.class);

      found = cacheQuery.execute().list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testAdded() {
      loadTestingData();
      Query<Person> cacheQuery = createQuery("name:'Goat'", Person.class);
      QueryResult<Person> result = cacheQuery.execute();
      List<Person> found = result.list();

      assertEquals(2, found.size());
      assertEquals(2, getNumberOfHits(result));
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));
      assertFalse(found.contains(person4));

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");

      cache.put("mighty", person4);

      cacheQuery = createQuery("name:'Goat'", Person.class);
      found = cacheQuery.execute().list();

      assert found.size() == 3 : "Size of list should be 3";
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testRemoved() {
      loadTestingData();

      Query<Person> cacheQuery = createQuery("name:'Goat'", Person.class);
      QueryResult<Person> result = cacheQuery.execute();
      List<Person> found = result.list();

      assertEquals(2, found.size());
      assertEquals(2, getNumberOfHits(result));
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));

      cache.remove(key3);

      cacheQuery = createQuery("name:'Goat'", Person.class);
      found = cacheQuery.execute().list();

      assertEquals(1, found.size());
      assertTrue(found.contains(person2));
      assertFalse(found.contains(person3));
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testUpdated() {
      loadTestingData();
      Query<Person> cacheQuery = createQuery("name:'Goat'", Person.class);
      QueryResult<Person> result = cacheQuery.execute();
      List<Person> found = result.list();

      assertEquals(2, found.size());
      assertEquals(2, getNumberOfHits(result));
      assertTrue(found.contains(person2));

      cache.put(key2, person1);

      cacheQuery = createQuery("name:'Goat'", Person.class);
      result = cacheQuery.execute();
      found = result.list();

      assertEquals(1, found.size());
      assertEquals(1, getNumberOfHits(result));
      assertFalse(found.contains(person2));
      assertFalse(found.contains(person1));
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testSetSort() {
      loadTestingData();
      String queryString = String.format("FROM %s p WHERE p.name:'Goat' ORDER BY p.age", Person.class.getName());
      Query<Person> cacheQuery = Search.<Person>getQueryFactory(cache).create(queryString);
      QueryResult<Person> result = cacheQuery.execute();
      List<Person> found = result.list();

      assertEquals(2, found.size());

      found = cacheQuery.execute().list();

      assertEquals(2, found.size());
      assertEquals(person3, found.get(0)); // person3 is 25 and named Goat
      assertEquals(person2, found.get(1)); // person2 is 30 and named Goat

      StaticTestingErrorHandler.assertAllGood(cache);

      //Now change the stored values:
      person2.setAge(10);
      cache.put(key2, person2);

      found = cacheQuery.execute().list();

      assertEquals(2, found.size());

      found = cacheQuery.execute().list();

      assertEquals(2, found.size());
      assertEquals(person2, found.get(0)); // person2 is 10 and named Goat
      assertEquals(person3, found.get(1)); // person3 is 25 and named Goat

      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testSetFilter() {
      loadTestingData();

      Query<?> cacheQuery = createQuery("name:'goat'", Person.class);
      List<?> found = cacheQuery.execute().list();

      assertEquals(2, found.size());

      cacheQuery = createQuery("name:'goat' AND blurb:'cheese'", Person.class);

      found = cacheQuery.execute().list();

      assertEquals(1, found.size());
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testLazyIterator() {
      loadTestingData();
      Query<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (CloseableIterator<?> found = cacheQuery.iterator()) {
         assertTrue(found.hasNext());
         found.next();
         assertFalse(found.hasNext());
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testIteratorWithDefaultOptions() {
      loadTestingData();
      Query<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (CloseableIterator<?> found = cacheQuery.iterator()) {
         assertTrue(found.hasNext());
         found.next();
         assertFalse(found.hasNext());
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testIteratorRemove() {
      loadTestingData();

      Query<?> cacheQuery = createQuery("blurb:'Eats'", Person.class);
      try (CloseableIterator<?> iterator = cacheQuery.iterator()) {
         if (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
         }
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testLazyIteratorWithOffset() {
      loadTestingData();
      Query<?> cacheQuery = createQuery("blurb:'Eats'", Person.class).startOffset(1);

      try (CloseableIterator<?> iterator = cacheQuery.iterator()) {
         assertEquals(1, countElements(iterator));
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testSearchManagerWithNullCache() {
      Search.getQueryFactory(null);
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testLazyIteratorWithNoElementsFound() {
      loadTestingData();
      Query<?> cacheQuery = createQuery("blurb:'fish'", Person.class).startOffset(1);

      try (CloseableIterator<?> found = cacheQuery.iterator()) {
         found.next();
      }
   }

   public void testSearchKeyTransformer() {
      loadTestingDataWithCustomKey();

      Query<Person> cacheQuery = createQuery("blurb:'Eats'", Person.class);

      try (CloseableIterator<Person> iterator = cacheQuery.iterator()) {
         assertEquals(2, countElements(iterator));
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testSearchWithWrongCache() {
      Cache<?, ?> cache = mock(CacheImpl.class);
      when(cache.getAdvancedCache()).thenReturn(null);

      Search.getQueryFactory(cache);
   }

   //Another test just for covering Search.java instantiation, although it is unnecessary. As well as covering the
   //valueOf() method of FetchMode, again just for adding coverage.
   public void testSearchManagerWithInstantiation() {
      loadTestingData();

      Query<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (CloseableIterator<?> found = cacheQuery.iterator()) {
         assertTrue(found.hasNext());
         found.next();
         assertFalse(found.hasNext());
      }
   }

   public void testGetResultSize() {
      loadTestingData();
      Query<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      assertEquals(1, getNumberOfHits(cacheQuery.execute()));
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testMaxResults() {
      loadTestingData();

      Query<Person> cacheQuery = createQuery("blurb:'eats'", Person.class).maxResults(1);

      QueryResult<Person> result = cacheQuery.execute();
      assertEquals(2, getNumberOfHits(result));   // NOTE: getResultSize() ignores pagination (maxResults, firstResult)
      assertEquals(1, result.list().size());

      try (CloseableIterator<?> eagerIterator = cacheQuery.iterator()) {
         assertEquals(1, countElements(eagerIterator));
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   private int countElements(Iterator<?> iterator) {
      int count = 0;
      while (iterator.hasNext()) {
         iterator.next();
         count++;
      }
      return count;
   }

   public void testClear() {
      loadTestingData();

      String predicate = "name:'navin' OR name:'goat'";
      Query<Person> cacheQuery = createQuery(predicate, Person.class);

      // We know that we've got all 3 hits.
      assertEquals(3, getNumberOfHits(cacheQuery.execute()));

      cache.clear();

      cacheQuery = createQuery(predicate, Person.class);

      assertEquals(0, getNumberOfHits(cacheQuery.execute()));
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testTypeFiltering() {
      loadTestingData();
      Query<?> cacheQuery = createQuery("blurb:'grass'", Person.class);

      List<?> found = cacheQuery.execute().list();

      assertEquals(1, found.size());
      assertTrue(found.contains(person2));

      cacheQuery = createQuery("blurb:'grass'", AnotherGrassEater.class);

      found = cacheQuery.execute().list();

      assertEquals(1, found.size());
      assertEquals(found.get(0), anotherGrassEater);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .indexing()
            .enable()
            .addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());
      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   @Override
   protected void teardown() {
      if (cache != null) {
         // a proper cache.clear() should ensure indexes and stores are cleared too if present
         // this is better and more complete than the cleanup performed by the superclass
         cache.clear();
      }
      super.teardown();
   }

   protected void loadTestingData() {
      prepareTestingData();

      cache.put(key1, person1);

      // person2 is verified as number of matches in multiple tests,
      // so verify duplicate insertion doesn't affect result counts:
      cache.put(key2, person2);
      cache.put(key2, person2);
      cache.put(key2, person2);

      cache.put(key3, person3);
      cache.put(anotherGrassEaterKey, anotherGrassEater);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   protected void loadTestingDataWithCustomKey() {
      prepareTestingData();

      CustomKey3 customeKey1 = new CustomKey3(key1);
      CustomKey3 customeKey2 = new CustomKey3(key2);
      CustomKey3 customeKey3 = new CustomKey3(key3);
      cache.put(customeKey1, person1);

      // person2 is verified as number of matches in multiple tests,
      // so verify duplicate insertion doesn't affect result counts:
      cache.put(customeKey2, person2);
      cache.put(customeKey2, person2);
      cache.put(customeKey2, person2);

      cache.put(customeKey3, person3);
      cache.put(anotherGrassEaterKey, anotherGrassEater);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   private void prepareTestingData() {
      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setAge(20);
      person1.setBlurb("Likes playing WoW");
      person1.setNonIndexedField("test1");

      person2 = new Person();
      person2.setName("Big Goat");
      person2.setAge(30);
      person2.setBlurb("Eats grass");
      person2.setNonIndexedField("test2");

      person3 = new Person();
      person3.setName("Mini Goat");
      person3.setAge(25);
      person3.setBlurb("Eats cheese");
      person3.setNonIndexedField("test3");

      anotherGrassEater = new AnotherGrassEater("Another grass-eater", "Eats grass");
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   protected void enhanceConfig(ConfigurationBuilder c) {
      // no op, meant to be overridden
   }

   private long getNumberOfHits(QueryResult<?> queryResult) {
      return queryResult.hitCount().orElse(-1);
   }
}
