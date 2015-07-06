package org.infinispan.all.embeddedquery;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.PrefixFilter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.hibernate.search.filter.FullTextFilter;
import org.infinispan.all.embeddedquery.testdomain.Person;
import org.infinispan.all.embeddedquery.testdomain.StaticTestingErrorHandler;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Clone of LocalCacheTest for uber-jars.
 *
 * @author Jiri Holusa (jholusa@redhat.com)
 */
public class LocalCacheTest extends AbstractQueryTest {

   private Person person1;
   private Person person2;
   private Person person3;
   private Person person4;
   private String key1 = "Navin";
   private String key2 = "BigGoat";
   private String key3 = "MiniGoat";

   @Before
   public void init() throws Exception {
      cache = createCacheManager().getCache();
   }

   @After
   public void after() {
      cache.clear();
   }

   @Test
   public void testSimple() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing" );

      List<Object> found = cacheQuery.list();

      assertPeopleInList(found, person1);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   public void testSimpleForNonField() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "nonSearchableField", "test1");
      List<Object> found = cacheQuery.list();

      int elems = found.size();
      assertEquals("Expected 0 but was " + elems, 0, elems);
      StaticTestingErrorHandler.assertAllGood(cache);
   }
   
   @Test
   public void testEagerIterator() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing" );

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));

      try {
         assertTrue(found.hasNext());
         found.next();
         assertTrue(!found.hasNext());
      } finally {
         found.close();
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testEagerIteratorRemove() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing" );

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));

      try {
         assertTrue(found.hasNext());
         found.remove();
      } finally {
         found.close();
      }
   }

   @Test(expected = NoSuchElementException.class)
   public void testEagerIteratorExCase() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing" );

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));

      try {
         assertTrue(found.hasNext());
         found.next();
         assertTrue(!found.hasNext());
         found.next();
      } finally {
         found.close();
      }
   }

   @Test
   public void testModified() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing");

      List<Object> found = cacheQuery.list();
      assertPeopleInList(found, person1);

      person1.setBlurb("Likes pizza");
      cache.put(key1, person1);

      cacheQuery = createCacheQuery(cache, "blurb", "pizza");

      found = cacheQuery.list();

      assertPeopleInList(found, person1);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   public void testAdded() {
      assertIndexingKnows(cache);
      loadTestingData();
      assertIndexingKnows(cache, Person.class);

      CacheQuery cacheQuery = createCacheQuery(cache, "name", "Goat");
      List<Object> found = cacheQuery.list();

      assertPeopleInList(found, person2, person3);

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");

      cache.put("mighty", person4);

      cacheQuery = createCacheQuery(cache, "name", "Goat");
      found = cacheQuery.list();

      assertPeopleInList(found, person2, person3, person4);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   public void testRemoved() {
      loadTestingData();

      CacheQuery cacheQuery = createCacheQuery(cache, "name", "Goat");
      List<Object> found = cacheQuery.list();

      assertPeopleInList(found, person2, person3);

      cache.remove(key3);

      cacheQuery = createCacheQuery(cache, "name", "Goat");
      found = cacheQuery.list();

      assertPeopleInList(found, person2);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   public void testUpdated() {
      loadTestingData();

      CacheQuery cacheQuery = createCacheQuery(cache, "name", "Goat");
      List<Object> found = cacheQuery.list();

      assertPeopleInList(found, person2, person3);

      cache.put(key2, person1);

      cacheQuery = createCacheQuery(cache, "name", "Goat");
      found = cacheQuery.list();

      assertPeopleInList(found, person3);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   public void testSetSort() {
      loadTestingData();

      Sort sort = new Sort( new SortField("age", SortField.Type.STRING));

      {
         CacheQuery cacheQuery = createCacheQuery(cache, "name", "Goat");
         List<Object> found = cacheQuery.list();

         assertPeopleInList(found, person2, person3);

         cacheQuery.sort( sort );
         found = cacheQuery.list();

         // person3 is 25 and named Goat
         // person2 is 30 and named Goat
         assertPeopleInSortedList(found, person3, person2);
      }
      StaticTestingErrorHandler.assertAllGood(cache);

      //Now change the stored values:
      person2.setAge(10);
      cache.put(key2, person2);

      {
         CacheQuery cacheQuery = createCacheQuery(cache, "name", "Goat");
         List<Object> found = cacheQuery.list();

         assertPeopleInList(found, person2, person3);

         cacheQuery.sort( sort );

         found = cacheQuery.list();

         // person2 is 30 and named Goat
         // person3 is 25 and named Goat
         assertPeopleInSortedList(found, person2, person3);
      }

      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   public void testSetFilter() {
      loadTestingData();

      CacheQuery cacheQuery = createCacheQuery(cache, "name", "Goat");
      List<Object> found = cacheQuery.list();

      assertPeopleInList(found, person2, person3);

      Filter filter = new PrefixFilter(new Term("blurb", "cheese"));

      cacheQuery.filter(filter);

      found = cacheQuery.list();

      assertPeopleInList(found, person3);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   public void testLazyIterator() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing");

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));

      try {
         assertTrue(found.hasNext());
         found.next();
         assertTrue(!found.hasNext());
      } finally {
         found.close();
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expected = IllegalArgumentException.class)
   public void testUnknownFetchModeIterator() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing");

      ResultIterator found = cacheQuery.iterator(new FetchOptions(){
         public FetchOptions fetchMode(FetchMode fetchMode) {
            return null;
         }
      });

      try {
         assertTrue(found.hasNext());
         found.next();
         assertTrue(!found.hasNext());
      } finally {
         found.close();
      }
   }

   @Test
   public void testIteratorWithDefaultOptions() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing");

      ResultIterator found = cacheQuery.iterator();

      try {
         assertTrue(found.hasNext());
         found.next();
         assertTrue(!found.hasNext());
      } finally {
         found.close();
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   public void testExplain() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "Eats");

      int matchCounter = 0;
      int i = 0;

      //The implementation is changed to this way as in case of NRT index manager the number of created documents may
      //differ comparing to the simple configuration.
      // Note: we cannot get rid of this dirty loop since there is no way how to get all the
      // explanations.
      while (true) {
         try {
            Explanation found = cacheQuery.explain(i);

            if (found.isMatch())
               matchCounter++;

            i++;
            if (i >= 10 || matchCounter == 2)
                break;
         } catch(ArrayIndexOutOfBoundsException ex) {
            break;
         }
      }

      assertEquals(2, matchCounter);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   public void testFullTextFilterOnOff() {
      loadTestingData();

      CacheQuery query = createCacheQuery(cache, "blurb", "Eats");
      FullTextFilter filter = query.enableFullTextFilter("personFilter");
      filter.setParameter("blurbText", "cheese");

      List found = query.list();
      assertPeopleInList(found, person3);

      //Disabling the fullTextFilter.
      query.disableFullTextFilter("personFilter");

      found = query.list();
      assertPeopleInList(found, person2, person3);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expected = UnsupportedOperationException.class)
   public void testIteratorRemove() {
      loadTestingData();

      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "Eats");
      ResultIterator iterator = cacheQuery.iterator();
      try {
         if (iterator.hasNext()) {
            Object next = iterator.next();
            iterator.remove();
         }
      } finally {
         iterator.close();
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test(expected = IllegalArgumentException.class)
   public void testSearchManagerWithNullCache() {
      loadTestingData();
      Query luceneQuery = new BooleanQuery();

      CacheQuery cacheQuery = Search.getSearchManager(null).getQuery(luceneQuery).firstResult(1);
   }

   @Test(expected = IllegalArgumentException.class)
   public void testLazyIteratorWithInvalidFetchSize() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "Eats").firstResult(1);

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY).fetchSize(0));
   }

   @Test(expected = NoSuchElementException.class)
   public void testLazyIteratorWithNoElementsFound() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "fish").firstResult(1);

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));

      try {
         found.next();
      } finally {
         found.close();
      }
   }

   @Test(expected = IllegalArgumentException.class)
   public void testIteratorWithNullFetchMode() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "Eats").firstResult(1);

      ResultIterator found = cacheQuery.iterator(new FetchOptions().fetchMode(null));

      try {
         found.next();
      } finally {
         found.close();
      }
   }

   @Test
   public void testGetResultSize() {
      loadTestingData();
      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "playing");

      assertEquals(1, cacheQuery.getResultSize());
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   public void testMaxResults() {
      loadTestingData();

      CacheQuery cacheQuery = createCacheQuery(cache, "blurb", "eats").maxResults(1);

      assertEquals(2, cacheQuery.getResultSize());   // NOTE: getResultSize() ignores pagination (maxResults, firstResult)
      assertEquals(1, cacheQuery.list().size());
      ResultIterator eagerIterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));
      try {
         assertEquals(1, countElements(eagerIterator));
      } finally {
         eagerIterator.close();
      }
      ResultIterator lazyIterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));
      try {
         assertEquals(1, countElements(lazyIterator));
      } finally {
         lazyIterator.close();
      }
      ResultIterator defaultIterator = cacheQuery.iterator();
      try {
         assertEquals(1, countElements(defaultIterator));
      } finally {
         defaultIterator.close();
      }
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   private int countElements(ResultIterator iterator) {
      int count = 0;
      while (iterator.hasNext()) {
         iterator.next();
         count++;
      }
      return count;
   }

   @Test
   public void testClear() {
      loadTestingData();

      // Create a term that will return me everyone called Navin.
      Term navin = new Term("name", "navin");

      // Create a term that I know will return me everything with name goat.
      Term goat = new Term ("name", "goat");

      BooleanQuery luceneQuery = new BooleanQuery();
      luceneQuery.add(new TermQuery(goat), Occur.SHOULD);
      luceneQuery.add(new TermQuery(navin), Occur.SHOULD);
      CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      // We know that we've got all 3 hits.
      assertEquals("Expected 3, got " + cacheQuery.getResultSize(), 3, cacheQuery.getResultSize());

      cache.clear();

      cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery);

      assertEquals(0, cacheQuery.getResultSize());
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   @Test
   public void testEntityDiscovery() {
      assertIndexingKnows(cache);

      Person p = new Person();
      p.setName("Lucene developer");
      p.setAge(30);
      p.setBlurb("works best on weekends");
      cache.put(p.getName(), p);
      
      assertIndexingKnows(cache, Person.class);
   }

   private void loadTestingData() {
      prepareTestingData();

      cache.put(key1, person1);

      // person2 is verified as number of matches in multiple tests,
      // so verify duplicate insertion doesn't affect result counts:
      cache.put(key2, person2);
      cache.put(key2, person2);
      cache.put(key2, person2);

      cache.put(key3, person3);
      StaticTestingErrorHandler.assertAllGood(cache);
   }

   private void prepareTestingData() {
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
   }

   private void assertPeopleInSortedList(List<Object> actualList, Object... expected) {
      List<Object> expectedList = new ArrayList<Object>(Arrays.asList(expected));
      assertEquals("Result doesn't match expected result.", expectedList, actualList);
   }

   private void assertPeopleInList(List<Object> actualList, Object... expected) {
      List<Object> expectedList = new ArrayList<Object>(Arrays.asList(expected));
      assertEquals("Size of the result doesn't match.", expected.length, actualList.size());

      //collections are not disjoint = they are the same
      assertTrue("Result doesn't match expected result. Expected: <" + expectedList + ">, was: <" + actualList + ">",
                 !Collections.disjoint(actualList, expectedList));
   }

}
