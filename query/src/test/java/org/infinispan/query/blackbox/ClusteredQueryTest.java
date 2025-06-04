package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.lucene.index.IndexReader;
import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.HashConfiguration;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.query.helper.IndexAccessor;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * ClusteredQueryTest.
 * @author Israel Lacerra &lt;israeldl@gmail.com&gt;
 * @since 5.1
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredQueryTest")
public class ClusteredQueryTest extends MultipleCacheManagersTest {

   static final int NUM_ENTRIES = 50;

   Cache<String, Person> cacheAMachine1, cacheAMachine2;
   private Query<Person> cacheQuery;
   protected String queryString = String.format("FROM %s where blurb:'blurb1?'", Person.class.getName());
   private final String allPersonsQuery = "FROM " + Person.class.getName();

   @Override
   public Object[] factory() {
      return new Object[]{
            new ClusteredQueryTest().storageType(StorageType.OFF_HEAP),
            new ClusteredQueryTest().storageType(StorageType.HEAP),
      };
   }

   @AfterMethod
   @Override
   protected void clearContent() {
      // Leave it be
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(getCacheMode(), false);
      cacheCfg
            .clustering().hash().numOwners(numOwners())
            .indexing().enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class);
      if (storageType != null) {
         cacheCfg.memory()
               .storage(storageType);
      }
      createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);
      cacheAMachine1 = cache(0);
      cacheAMachine2 = cache(1);
      populateCache();
   }

   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   protected int numOwners() {
      return HashConfiguration.NUM_OWNERS.getDefaultValue();
   }

   protected void prepareTestData() {
      IntStream.range(0, NUM_ENTRIES).boxed()
            .map(i -> new Person("name" + i, "blurb" + i, i))
            .forEach(p -> {
               Cache<String, Person> cache = p.getAge() % 2 == 0 ? cacheAMachine1 : cacheAMachine2;
               cache.put(p.getName(), p);
            });

      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   // TODO HSEARCH-3323 Restore support for scrolling
   @Test(enabled = false)
   public void testLazyOrdered() {
      Query<Person> cacheQuery = createSortedQuery();

      for (int i = 0; i < 2; i++) {
         try (CloseableIterator<Person> iterator = cacheQuery.iterator()) {
            assertEquals(10, cacheQuery.execute().count().value());

            int previousAge = 0;
            while (iterator.hasNext()) {
               Person person = iterator.next();
               assertTrue(person.getAge() > previousAge);
               previousAge = person.getAge();
            }
         }
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   // TODO HSEARCH-3323 Restore support for scrolling
   @Test(enabled = false)
   public void testLazyNonOrdered() {
      try (CloseableIterator<Person> ignored = cacheQuery.iterator()) {
         assertEquals(10, cacheQuery.execute().count().value());
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testLocalQuery() {
      final Query<Person> localQuery1 = cacheAMachine1.query(queryString);
      List<Person> results1 = localQuery1.execute().list();

      final Query<Person> localQuery2 = cacheAMachine1.query(queryString);
      List<Person> results2 = localQuery2.execute().list();

      assertEquals(10, results1.size());
      assertEquals(10, results2.size());

      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testEagerOrdered() {
      Query<Person> cacheQuery = createSortedQuery();

      try (CloseableIterator<Person> iterator = cacheQuery.iterator()) {
         assertEquals(10, cacheQuery.execute().count().value());

         int previousAge = 0;
         while (iterator.hasNext()) {
            Person person = iterator.next();
            assertTrue(person.getAge() > previousAge);
            previousAge = person.getAge();
         }
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testIteratorNextOutOfBounds() {
      cacheQuery.maxResults(1);
      try (CloseableIterator<Person> iterator = cacheQuery.iterator()) {
         assertTrue(iterator.hasNext());
         iterator.next();

         assertFalse(iterator.hasNext());
         iterator.next();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testIteratorRemove() {
      cacheQuery.maxResults(1);
      try (CloseableIterator<Person> iterator = cacheQuery.iterator()) {
         assert iterator.hasNext();
         iterator.remove();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testList() {
      Query<Person> cacheQuery = createSortedQuery();

      QueryResult<Person> result = cacheQuery.execute();
      List<Person> results = result.list();
      assertEquals(10, result.count().value());

      int previousAge = 0;
      for (Person person : results) {
         assertTrue(person.getAge() > previousAge);
         previousAge = person.getAge();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testGetResultSizeList() {
      assertEquals(10, cacheQuery.execute().list().size());
   }

   public void testPagination() {
      Query<Person> cacheQuery = createSortedQuery();
      cacheQuery.startOffset(2);
      cacheQuery.maxResults(1);

      QueryResult<Person> results = cacheQuery.execute();
      List<Person> list = results.list();
      assertEquals(1, list.size());
      assertEquals(10, results.count().value());
      Person result = list.get(0);
      assertEquals(12, result.getAge());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   @Test
   public void testPagination2() {
      int[] pageSizes = new int[]{1, 5, 7, NUM_ENTRIES + 10};

      for (int pageSize : pageSizes) {
         testPaginationWithoutSort(pageSize);
         testPaginationWithSort(pageSize, "age");
      }
   }

   private void testPaginationWithoutSort(int pageSize) {
      testPaginationInternal(pageSize, null);
   }

   private void testPaginationWithSort(int pageSize, String sortField) {
      testPaginationInternal(pageSize, sortField);
   }

   private void testPaginationInternal(int pageSize, String sortField) {
      Query<Person> paginationQuery = buildPaginationQuery(0, pageSize, sortField);

      int idx = 0;
      Set<String> keys = new HashSet<>();
      while (idx < NUM_ENTRIES) {
         List<Person> results = paginationQuery.execute().list();
         results.stream().map(Person::getName).forEach(keys::add);
         idx += pageSize;
         paginationQuery = buildPaginationQuery(idx, pageSize, sortField);
      }

      assertEquals(NUM_ENTRIES, keys.size());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   private Query<Person> buildPaginationQuery(int offset, int pageSize, String sortField) {
      String sortedQuery = sortField != null ? allPersonsQuery + " ORDER BY " + sortField : allPersonsQuery;
      Query<Person> clusteredQuery = cacheAMachine1.query(sortedQuery);
      clusteredQuery.startOffset(offset);
      clusteredQuery.maxResults(pageSize);
      return clusteredQuery;
   }

   public void testQueryAll() {
      Query<Person> clusteredQuery = cacheAMachine1.query(allPersonsQuery);

      assertEquals(NUM_ENTRIES, clusteredQuery.execute().list().size());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testFuzzyQuery() {
      populateCache();

      String q = String.format("FROM %s WHERE name:'name1'~2", Person.class.getName());

      Query<Person> clusteredQuery = cacheAMachine1.query(q);

      assertEquals(NUM_ENTRIES, clusteredQuery.execute().list().size());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastIckleMatchAllQuery() {
      Query<Person> everybody = cacheAMachine1.query(String.format("FROM %s", Person.class.getName()));

      assertEquals(NUM_ENTRIES, everybody.execute().list().size());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastIckleTermQuery() {
      String targetName = "name2";
      Query<Person> singlePerson = cacheAMachine1.query(String.format("FROM %s p where p.name:'%s'", Person.class.getName(), targetName));

      assertEquals(1, singlePerson.execute().list().size());
      assertEquals(targetName, singlePerson.iterator().next().getName());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastFuzzyIckle() {
      Query<Person> persons0to10 = cacheAMachine1.query(String.format("FROM %s p where p.name:'nome'~2", Person.class.getName()));

      assertEquals(10, persons0to10.execute().list().size());

      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastNumericRangeQuery() {
      Query<Person> infants = cacheAMachine1
            .query(String.format("FROM %s p where p.age between 0 and 5", Person.class.getName()));

      assertEquals(6, infants.execute().list().size());

      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastProjectionIckleQuery() {
      Query<Object[]> onlyNames = cacheAMachine2.query(
            String.format("Select p.name FROM %s p", Person.class.getName()));

      List<Object[]> results = onlyNames.execute().list();
      assertEquals(NUM_ENTRIES, results.size());

      Set<String> names = new HashSet<>();
      results.iterator().forEachRemaining(s -> names.add((String) s[0]));

      Set<String> allNames = IntStream.range(0, NUM_ENTRIES).boxed().map(i -> "name" + i).collect(Collectors.toSet());
      assertEquals(allNames, names);
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastSortedIckleQuery() {
      Query<Person> theLastWillBeFirst = cacheAMachine2.query(
            String.format("FROM %s p order by p.age desc", Person.class.getName()));

      List<Person> results = theLastWillBeFirst.execute().list();
      assertEquals(NUM_ENTRIES, results.size());
      assertEquals(NUM_ENTRIES - 1, results.iterator().next().getAge());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testPaginatedIckleQuery() {
      Query<Person> q = cacheAMachine1.query(String.format("FROM %s p order by p.age", Person.class.getName()));

      q.startOffset(5);
      q.maxResults(10);

      List<Person> results = q.execute().list();

      assertEquals(10, results.size());
      assertEquals("name5", results.iterator().next().getName());
      assertEquals("name14", results.get(9).getName());
   }

   private int countLocalIndex(Cache<String, Person> cache) throws IOException {
      IndexReader indexReader = IndexAccessor.of(cache, Person.class).getIndexReader();
      return indexReader.numDocs();
   }

   @Test
   public void testHybridQueryWorks() {
      Query<Person> hybridQuery = cacheAMachine1
            .query(String.format("FROM %s p where p.nonIndexedField = 'nothing'", Person.class.getName()));

      hybridQuery.execute();
   }

   @Test
   public void testAggregationQueriesWork() {
      Query<Person> aggregationQuery = cacheAMachine1
            .query(String.format("SELECT p.name FROM %s p where p.name:'name3' group by p.name", Person.class.getName()));

      aggregationQuery.execute().list();
   }

   @Test
   public void testBroadcastNativeInfinispanHybridQuery() {
      String q = "FROM " + Person.class.getName() + " where age >= 40 and nonIndexedField = 'na'";
      Query<?> query = cacheAMachine1.query(q);

      assertEquals(10, query.execute().list().size());
   }

   @Test
   public void testBroadcastNativeInfinispanFuzzyQuery() {
      String q = String.format("FROM %s p where p.name:'nome'~2", Person.class.getName());
      Query<?> query = cacheAMachine1.query(q);

      assertEquals(10, query.execute().list().size());
   }

   @Test
   public void testBroadcastSortedInfinispanQuery() {
      Query<Person> sortedQuery = cacheAMachine1.query("FROM " + Person.class.getName() + " p order by p.age desc");

      List<Person> results = sortedQuery.execute().list();
      assertEquals(NUM_ENTRIES, results.size());
      assertEquals(NUM_ENTRIES - 1, results.iterator().next().getAge());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   // TODO HSEARCH-3323 Restore support for scrolling
   @Test(enabled = false)
   public void testIckleProjectionsLazyRetrieval() {
      String query = String.format("SELECT name, blurb FROM %s p ORDER BY age", Person.class.getName());

      Query<Object[]> cacheQuery = cacheAMachine1.query(query);
      CloseableIterator<Object[]> iterator = cacheQuery.iterator();

      List<Object[]> values = toList(iterator);

      for (int i = 0; i < NUM_ENTRIES; i++) {
         Object[] projection = values.get(i);
         assertEquals(projection[0], "name" + i);
         assertEquals(projection[1], "blurb" + i);
      }
   }

   private <E> List<E> toList(Iterator<E> iterator) {
      return StreamSupport.stream(((Iterable<E>) () -> iterator).spliterator(), false)
            .collect(Collectors.toList());
   }

   @Test
   public void testBroadcastAggregatedInfinispanQuery() {
      Query<Person> hybridQuery = cacheAMachine2.query("select name FROM " + Person.class.getName() + " WHERE name : 'na*' group by name");

      assertEquals(NUM_ENTRIES, hybridQuery.execute().list().size());
   }

   @Test
   public void testNonIndexedBroadcastInfinispanQuery() {
      Query<Person> slowQuery = cacheAMachine2.query("FROM " + Person.class.getName() + " WHERE nonIndexedField LIKE 'na%'");

      assertEquals(NUM_ENTRIES, slowQuery.execute().list().size());
   }

   protected void populateCache() {
      prepareTestData();

      cacheQuery = cacheAMachine1.query(queryString);
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   protected Query<Person> createSortedQuery() {
      String q = String.format("FROM %s p where p.blurb:'blurb1?' order by p.age'", Person.class.getName());
      return cacheAMachine1.query(q);
   }
}
