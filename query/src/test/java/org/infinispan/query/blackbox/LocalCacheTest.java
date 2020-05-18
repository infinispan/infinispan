package org.infinispan.query.blackbox;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.hibernate.search.filter.FullTextFilter;
import org.infinispan.Cache;
import org.infinispan.cache.impl.CacheImpl;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
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

   private CacheQuery<Person> createQuery(String predicate, Class<?> entity) {
      SearchManager searchManager = Search.getSearchManager(cache);
      return searchManager.getQuery(String.format("FROM %s WHERE %s", entity.getName(), predicate));
   }

   public void testSimple() {
      loadTestingData();
      CacheQuery<Person> cacheQuery = createQuery("blurb:'playing'", Person.class);

      List<Person> found = cacheQuery.list();

      int elems = found.size();
      assert elems == 1 : "Expected 1 but was " + elems;

      Person val = found.get(0);
      assert val.equals(person1) : "Expected " + person1 + " but was " + val;
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testEagerIterator() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (ResultIterator<?> found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER))) {
         assertTrue(found.hasNext());
         found.next();
         assertFalse(found.hasNext());
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testEagerIteratorRemove() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (ResultIterator<?> found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER))) {
         assertTrue(found.hasNext());
         found.remove();
      }
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testEagerIteratorExCase() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (ResultIterator<?> found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER))) {
         assertTrue(found.hasNext());
         found.next();
         assertFalse(found.hasNext());
         found.next();
      }
   }

   public void testMultipleResults() {
      loadTestingData();

      CacheQuery<Person> cacheQuery = createQuery("name:'goat'", Person.class);
      List<Person> found = cacheQuery.list();

      assert found.size() == 2;
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testModified() {
      loadTestingData();
      CacheQuery<Person> cacheQuery = createQuery("blurb:'playing'", Person.class);

      List<Person> found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);

      person1.setBlurb("Likes pizza");
      cache.put(key1, person1);

      cacheQuery = createQuery("blurb:'pizza'", Person.class);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testAdded() {
      loadTestingData();
      CacheQuery<Person> cacheQuery = createQuery("name:'Goat'", Person.class);
      List<Person> found = cacheQuery.list();

      assert found.size() == 2 : "Size of list should be 2";
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");

      cache.put("mighty", person4);

      cacheQuery = createQuery("name:'Goat'", Person.class);
      found = cacheQuery.list();

      assert found.size() == 3 : "Size of list should be 3";
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testRemoved() {
      loadTestingData();

      CacheQuery<Person> cacheQuery = createQuery("name:'Goat'", Person.class);
      List<Person> found = cacheQuery.list();

      assert found.size() == 2;
      assert found.contains(person2);
      assert found.contains(person3) : "This should still contain object person3";

      cache.remove(key3);

      cacheQuery = createQuery("name:'Goat'", Person.class);
      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.contains(person2);
      assert !found.contains(person3) : "The search should not return person3";
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testUpdated() {
      loadTestingData();
      CacheQuery<Person> cacheQuery = createQuery("name:'Goat'", Person.class);
      List<Person> found = cacheQuery.list();

      assert found.size() == 2 : "Size of list should be 2";
      assert found.contains(person2) : "The search should have person2";

      cache.put(key2, person1);

      cacheQuery = createQuery("name:'Goat'", Person.class);
      found = cacheQuery.list();

      assert found.size() == 1 : "Size of list should be 1";
      assert !found.contains(person2) : "Person 2 should not be found now";
      assert !found.contains(person1) : "Person 1 should not be found because it does not meet the search criteria";
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testSetSort() {
      loadTestingData();

      Sort sort = new Sort(new SortField("age", SortField.Type.INT));

      CacheQuery<Person> cacheQuery = createQuery("name:'Goat'", Person.class);
      List<Person> found = cacheQuery.list();

      assert found.size() == 2;

      cacheQuery.sort(sort);

      found = cacheQuery.list();

      assert found.size() == 2;
      assert found.get(0).equals(person3); // person3 is 25 and named Goat
      assert found.get(1).equals(person2); // person2 is 30 and named Goat

      StaticTestingErrorHandler.assertAllGood(cache);

      //Now change the stored values:
      person2.setAge(10);
      cache.put(key2, person2);

      cacheQuery = createQuery("name:'Goat'", Person.class);
      found = cacheQuery.list();

      assert found.size() == 2;

      cacheQuery.sort(sort);

      found = cacheQuery.list();

      assert found.size() == 2;
      assert found.get(0).equals(person2); // person2 is 10 and named Goat
      assert found.get(1).equals(person3); // person3 is 25 and named Goat

      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testSetFilter() {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("name:'goat'", Person.class);
      List<?> found = cacheQuery.list();

      assert found.size() == 2;

      cacheQuery = createQuery("name:'goat' AND blurb:'cheese'", Person.class);

      found = cacheQuery.list();

      assert found.size() == 1;
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testLazyIterator() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (ResultIterator<?> found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY))) {
         assertTrue(found.hasNext());
         found.next();
         assertFalse(found.hasNext());
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Unknown FetchMode null")
   public void testUnknownFetchModeIterator() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (ResultIterator<?> found = cacheQuery.iterator(new FetchOptions() {
         public FetchMode getFetchMode() {
            return null;
         }
      })) {
         assertTrue(found.hasNext());
         found.next();
         assertFalse(found.hasNext());
      }
   }

   public void testIteratorWithDefaultOptions() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (ResultIterator<?> found = cacheQuery.iterator()) {
         assertTrue(found.hasNext());
         found.next();
         assertFalse(found.hasNext());
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testExplain() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'Eats'", Person.class);
      Explanation found = cacheQuery.explain(0);
      assertNotNull(found);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testFullTextFilterOnOff() {
      loadTestingData();

      CacheQuery<Person> query = createQuery("blurb:'Eats'", Person.class);
      FullTextFilter filter = query.enableFullTextFilter("personFilter");
      filter.setParameter("blurbText", "cheese");

      assertEquals(1, query.getResultSize());
      List<Person> result = query.list();

      Person person = result.get(0);
      assertEquals("Mini Goat", person.getName());
      assertEquals("Eats cheese", person.getBlurb());

      //Disabling the fullTextFilter.
      query.disableFullTextFilter("personFilter");
      assertEquals(2, query.getResultSize());
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testIteratorRemove() {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("blurb:'Eats'", Person.class);
      try (ResultIterator<?> iterator = cacheQuery.iterator()) {
         if (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
         }
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testLazyIteratorWithOffset() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'Eats'", Person.class).firstResult(1);

      try (ResultIterator<?> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY))) {
         assertEquals(1, countElements(iterator));
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testSearchManagerWithNullCache() {
      Search.getSearchManager(null);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testLazyIteratorWithInvalidFetchSize() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'Eats'", Person.class).firstResult(1);

      cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY).fetchSize(0));
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testLazyIteratorWithNoElementsFound() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'fish'", Person.class).firstResult(1);

      try (ResultIterator<?> found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY))) {
         found.next();
      }
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIteratorWithNullFetchMode() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'Eats'", Person.class).firstResult(1);

      try (ResultIterator<?> found = cacheQuery.iterator(new FetchOptions().fetchMode(null))) {
         found.next();
      }
   }

   public void testSearchKeyTransformer() {
      loadTestingDataWithCustomKey();

      CacheQuery<?> cacheQuery = createQuery("blurb:'Eats'", Person.class);

      try (ResultIterator<?> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY))) {
         assertEquals(2, countElements(iterator));
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testSearchWithWrongCache() {
      Cache<?, ?> cache = mock(CacheImpl.class);
      when(cache.getAdvancedCache()).thenReturn(null);

      Search.getSearchManager(cache);
   }

   //Another test just for covering Search.java instantiation, although it is unnecessary. As well as covering the
   //valueOf() method of FetchMode, again just for adding coverage.
   public void testSearchManagerWithInstantiation() {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      try (ResultIterator<?> found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.valueOf("LAZY")))) {
         assertTrue(found.hasNext());
         found.next();
         assertFalse(found.hasNext());
      }
   }

   public void testGetResultSize() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'playing'", Person.class);

      assert cacheQuery.getResultSize() == 1;
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testMaxResults() {
      loadTestingData();

      CacheQuery<?> cacheQuery = createQuery("blurb:'eats'", Person.class).maxResults(1);

      assertEquals(2, cacheQuery.getResultSize());   // NOTE: getResultSize() ignores pagination (maxResults, firstResult)
      assertEquals(1, cacheQuery.list().size());
      try (ResultIterator<?> eagerIterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER))) {
         assertEquals(1, countElements(eagerIterator));
      }
      try (ResultIterator<?> lazyIterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY))) {
         assertEquals(1, countElements(lazyIterator));
      }
      try (ResultIterator<?> defaultIterator = cacheQuery.iterator()) {
         assertEquals(1, countElements(defaultIterator));
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
      CacheQuery<Person> cacheQuery = createQuery(predicate, Person.class);

      // We know that we've got all 3 hits.
      assert cacheQuery.getResultSize() == 3 : "Expected 3, got " + cacheQuery.getResultSize();

      cache.clear();

      cacheQuery = createQuery(predicate, Person.class);

      assert cacheQuery.getResultSize() == 0;
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   public void testTypeFiltering() {
      loadTestingData();
      CacheQuery<?> cacheQuery = createQuery("blurb:'grass'", Person.class);

      List<?> found = cacheQuery.list();

      assert found.size() == 1;
      assert found.contains(person2);

      cacheQuery = createQuery("blurb:'grass'", AnotherGrassEater.class);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(anotherGrassEater);
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
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", StaticTestingErrorHandler.class.getName())
            .addProperty("lucene_version", "LUCENE_CURRENT");
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
}
