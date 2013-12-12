package org.infinispan.query.blackbox;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.ParseException;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.hibernate.search.FullTextFilter;
import org.hibernate.search.engine.spi.EntityIndexBinding;
import org.hibernate.search.engine.spi.SearchFactoryImplementor;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.infinispan.Cache;
import org.infinispan.CacheImpl;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.spi.SearchManagerImplementor;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import java.util.NoSuchElementException;

import static java.util.Arrays.asList;
import static org.infinispan.query.helper.TestQueryHelperFactory.createCacheQuery;
import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Test(groups = "functional", testName = "query.blackbox.LocalCacheTest")
public class LocalCacheTest extends SingleCacheManagerTest {
   protected Person person1;
   protected Person person2;
   protected Person person3;
   protected Person person4;
   protected AnotherGrassEater anotherGrassEater;
   protected QueryParser queryParser;
   protected String key1 = "Navin";
   protected String key2 = "BigGoat";
   protected String key3 = "MiniGoat";
   protected String anotherGrassEaterKey = "anotherGrassEaterKey";
   
   public LocalCacheTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public void testSimple() throws ParseException {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing" );

      List<Object> found = cacheQuery.list();

      int elems = found.size();
      assert elems == 1 : "Expected 1 but was " + elems;

      Object val = found.get(0);
      assert val.equals(person1) : "Expected " + person1 + " but was " + val;
   }

   public void testSimpleForNonField() throws ParseException {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "nonSearchableField", "test1" );
      List<Object> found = cacheQuery.list();

      int elems = found.size();
      assert elems == 0 : "Expected 0 but was " + elems;
   }

   public void testEagerIterator() throws ParseException {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing" );

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));

      try {
         assert found.hasNext();
         found.next();
         assert !found.hasNext();
      } finally {
         found.close();
      }
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testEagerIteratorRemove() throws ParseException {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing" );

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));

      try {
         assert found.hasNext();
         found.remove();
      } finally {
         found.close();
      }
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testEagerIteratorExCase() throws ParseException {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing" );

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));

      try {
         assert found.hasNext();
         found.next();
         assert !found.hasNext();
         found.next();
      } finally {
         found.close();
      }
   }

   public void testMultipleResults() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("goat");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2;
      AssertJUnit.assertTrue(found.contains(person2));
      AssertJUnit.assertTrue(found.contains(person3));
   }

   public void testModified() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("playing");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      List<Object> found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);

      person1.setBlurb("Likes pizza");
      cache.put(key1, person1);

      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("pizza");
      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);
   }

   public void testAdded() throws ParseException {
      assertIndexingKnows(cache);
      loadTestingData();
      assertIndexingKnows(cache, Person.class, AnotherGrassEater.class);
      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("Goat");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2 : "Size of list should be 2";
      assert found.contains(person2);
      assert found.contains(person3);
      assert !found.contains(person4) : "This should not contain object person4";

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");

      cache.put("mighty", person4);

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 3 : "Size of list should be 3";
      assert found.contains(person2);
      assert found.contains(person3);
      assert found.contains(person4) : "This should now contain object person4";
   }

   public void testRemoved() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("Goat");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2;
      assert found.contains(person2);
      assert found.contains(person3) : "This should still contain object person3";

      cache.remove(key3);

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.contains(person2);
      assert !found.contains(person3) : "The search should not return person3";
   }

   public void testUpdated() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("Goat");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2 : "Size of list should be 2";
      assert found.contains(person2) : "The search should have person2";

      cache.put(key2, person1);

      luceneQuery = queryParser.parse("Goat");
      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      found = cacheQuery.list();
      
      assert found.size() == 1 : "Size of list should be 1";
      assert !found.contains(person2) : "Person 2 should not be found now";
      assert !found.contains(person1) : "Person 1 should not be found because it does not meet the search criteria";
   }

   public void testSetSort() throws ParseException {
      loadTestingData();

      Sort sort = new Sort( new SortField("age", SortField.STRING));

      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("Goat");
      {
         CacheQuery cacheQuery = Search.getSearchManager( cache ).getQuery( luceneQuery );
         List<Object> found = cacheQuery.list();

         assert found.size() == 2;

         cacheQuery.sort( sort );

         found = cacheQuery.list();

         assert found.size() == 2;
         assert found.get( 0 ).equals( person3 ); // person3 is 25 and named Goat
         assert found.get( 1 ).equals( person2 ); // person2 is 30 and named Goat
      }

      //Now change the stored values:
      person2.setAge(10);
      cache.put(key2, person2);

      {
         CacheQuery cacheQuery = Search.getSearchManager( cache ).getQuery( luceneQuery );
         List<Object> found = cacheQuery.list();

         assert found.size() == 2;

         cacheQuery.sort( sort );

         found = cacheQuery.list();

         assert found.size() == 2;
         assert found.get( 0 ).equals( person2 ); // person2 is 30 and named Goat
         assert found.get( 1 ).equals( person3 ); // person3 is 25 and named Goat
      }
   }

   public void testSetFilter() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("name");

      Query luceneQuery = queryParser.parse("goat");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      List<Object> found = cacheQuery.list();

      assert found.size() == 2;

      Filter filter = new PrefixFilter(new Term("blurb", "cheese"));

      cacheQuery.filter(filter);

      found = cacheQuery.list();

      assert found.size() == 1;
   }

   public void testLazyIterator() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("playing");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));

      try {
         assert found.hasNext();
         found.next();
         assert !found.hasNext();
      } finally {
         found.close();
      }
   }

   @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Unknown FetchMode null")
   public void testUnknownFetchModeIterator() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("playing");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      ResultIterator found = cacheQuery.iterator(new FetchOptions(){
         public FetchOptions fetchMode(FetchMode fetchMode) {
            return null;
         }
      });

      try {
         assert found.hasNext();
         found.next();
         assert !found.hasNext();
      } finally {
         found.close();
      }
   }

   public void testIteratorWithDefaultOptions() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("playing");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      ResultIterator found = cacheQuery.iterator();

      try {
         assert found.hasNext();
         found.next();
         assert !found.hasNext();
      } finally {
         found.close();
      }
   }

   public void testExplain() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("Eats");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      int matchCounter = 0;
      int i = 0;

      //The implementation is changed to this way as in case of NRT index manager the number of created documents may
      //differ comparing to the simple configuration.
      while (true) {
         try {
            Explanation found = cacheQuery.explain(i);

            if(found.isMatch())
               matchCounter++;

            i++;
         } catch(ArrayIndexOutOfBoundsException ex) {
            break;
         }
      }

      AssertJUnit.assertEquals(3, matchCounter);
   }

   public void testFullTextFilterOnOff() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("Eats");

      CacheQuery query = Search.getSearchManager(cache).getQuery(luceneQuery);
      FullTextFilter filter = query.enableFullTextFilter("personFilter");
      filter.setParameter("blurbText", "cheese");

      AssertJUnit.assertEquals(1, query.getResultSize());
      List result = query.list();

      Person person = (Person) result.get(0);
      AssertJUnit.assertEquals("Mini Goat", person.getName());
      AssertJUnit.assertEquals("Eats cheese", person.getBlurb());

      //Disabling the fullTextFilter.
      query.disableFullTextFilter("personFilter");
      AssertJUnit.assertEquals(3, query.getResultSize());
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testIteratorRemove() throws ParseException {
      loadTestingData();

      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("Eats");

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);
      ResultIterator iterator = cacheQuery.iterator();
      try {
         if (iterator.hasNext()) {
            Object next = iterator.next();
            iterator.remove();
         }
      } finally {
         iterator.close();
      }
   }

   public void testLazyIteratorWithOffset() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("Eats");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery).firstResult(1);

      ResultIterator iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));
      try {
         Assert.assertEquals(2, countElements(iterator));
      } finally {
         iterator.close();
      }
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testSearchManagerWithNullCache() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("fish");
      CacheQuery cacheQuery = Search.getSearchManager(null).getQuery(luceneQuery).firstResult(1);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testLazyIteratorWithInvalidFetchSize() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("Eats");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery).firstResult(1);

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY).fetchSize(0));
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testLazyIteratorWithNoElementsFound() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("fish");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery).firstResult(1);

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));

      try {
         found.next();
      } finally {
         found.close();
      }
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testIteratorWithNullFetchMode() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("Eats");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery).firstResult(1);

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(null));

      try {
         found.next();
      } finally {
         found.close();
      }
   }

   public void testSearchKeyTransformer() throws ParseException {
      SearchManagerImplementor manager = (SearchManagerImplementor) Search.getSearchManager(cache);
      manager.registerKeyTransformer(CustomKey3.class, CustomKey3Transformer.class);

      loadTestingDataWithCustomKey();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("Eats");

      CacheQuery cacheQuery = manager.getQuery(luceneQuery);

      ResultIterator iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));
      try {
         Assert.assertEquals(3, countElements(iterator));
      } finally {
         iterator.close();
      }
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testSearchWithWrongCache() throws ParseException {
      Cache cache = mock(CacheImpl.class);
      when(cache.getAdvancedCache()).thenReturn(null);

      SearchManager manager = Search.getSearchManager(cache);
   }

   //Another test just for covering Search.java instantiation, although it is unnecessary. As well as covering the
   //valueOf() method of FetchMode, again just for adding coverage.
   public void testSearchManagerWithInstantiation() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("playing");

      Search search = new Search();
      CacheQuery cacheQuery = search.getSearchManager(cache).getQuery(luceneQuery);

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.valueOf("LAZY")));

      try {
         assert found.hasNext();
         found.next();
         assert !found.hasNext();
      } finally {
         found.close();
      }
   }

   public void testGetResultSize() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("playing");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      assert cacheQuery.getResultSize() == 1;
   }

   public void testMaxResults() throws ParseException {
      loadTestingData();

      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("eats");

      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery)
            .maxResults(1);

      Assert.assertEquals(3, cacheQuery.getResultSize());   // NOTE: getResultSize() ignores pagination (maxResults, firstResult)
      Assert.assertEquals(1, cacheQuery.list().size());
      ResultIterator eagerIterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));
      try {
         Assert.assertEquals(1, countElements(eagerIterator));
      } finally {
         eagerIterator.close();
      }
      ResultIterator lazyIterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));
      try {
         Assert.assertEquals(1, countElements(lazyIterator));
      } finally {
         lazyIterator.close();
      }
      ResultIterator defaultIterator = cacheQuery.iterator();
      try {
         Assert.assertEquals(1, countElements(defaultIterator));
      } finally {
         defaultIterator.close();
      }
   }

   private int countElements(ResultIterator iterator) {
      int count = 0;
      while (iterator.hasNext()) {
         iterator.next();
         count++;
      }
      return count;
   }

   public void testClear() {
      loadTestingData();

      // Create a term that will return me everyone called Navin.
      Term navin = new Term("name", "navin");

      // Create a term that I know will return me everything with name goat.
      Term goat = new Term ("name", "goat");

      Query[] queries = new Query[2];
      queries[0] = new TermQuery(goat);
      queries[1] = new TermQuery(navin);

      Query luceneQuery = queries[0].combine(queries);
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      // We know that we've got all 3 hits.
      assert cacheQuery.getResultSize() == 3 : "Expected 3, got " + cacheQuery.getResultSize();

      cache.clear();

      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      assert cacheQuery.getResultSize() == 0;
   }

   public void testTypeFiltering() throws ParseException {
      loadTestingData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("grass");
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      List<Object> found = cacheQuery.list();

      assert found.size() == 2;
      assert found.containsAll(asList(person2, anotherGrassEater));

      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("grass");
      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery, AnotherGrassEater.class);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(anotherGrassEater);
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
      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }
   
   public void testEntityDiscovery() {
      assertIndexingKnows(cache);

      Person p = new Person();
      p.setName("Lucene developer");
      p.setAge(30);
      p.setBlurb("works best on weekends");
      cache.put(p.getName(), p);
      
      assertIndexingKnows(cache, Person.class);
   }

   /**
    * Verifies if the indexing interceptor is aware of a specific list of types.
    * @param cache the cache containing the indexes
    * @param types vararg listing the types the indexing engine should know
    */
   private void assertIndexingKnows(Cache<Object, Object> cache, Class... types) {
      ComponentRegistry cr = cache.getAdvancedCache().getComponentRegistry();
      SearchFactoryImplementor searchFactoryIntegrator = (SearchFactoryImplementor) cr.getComponent(SearchFactoryIntegrator.class);
      Assert.assertNotNull(searchFactoryIntegrator);
      Map<Class<?>, EntityIndexBinding> indexBindingForEntity = searchFactoryIntegrator.getIndexBindings();
      Assert.assertNotNull(indexBindingForEntity);
      Set<Class<?>> keySet = indexBindingForEntity.keySet();
      Assert.assertEquals(types.length, keySet.size());
      Assert.assertTrue(keySet.containsAll(Arrays.asList(types)));
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
   }

   protected void prepareTestingData() {
      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setAge(20);
      person1.setBlurb("Likes playing WoW");
      person1.setNonSearchableField("test1");

      person2 = new Person();
      person2.setName("Big Goat");
      person2.setAge(30);
      person2.setBlurb("Eats grass");
      person2.setNonSearchableField("test2");

      person3 = new Person();
      person3.setName("Mini Goat");
      person3.setAge(25);
      person3.setBlurb("Eats cheese");
      person3.setNonSearchableField("test3");

      anotherGrassEater = new AnotherGrassEater("Another grass-eater", "Eats grass");
   }
   
   protected void enhanceConfig(ConfigurationBuilder c) {
      // no op, meant to be overridden
   }

}
