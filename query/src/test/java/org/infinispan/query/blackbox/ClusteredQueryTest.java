package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.lucene.index.IndexReader;
import org.hibernate.search.engine.integration.impl.ExtendedSearchIntegrator;
import org.hibernate.search.exception.SearchException;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
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
   private CacheQuery<Person> cacheQuery;
   protected StorageType storageType;
   protected String queryString = String.format("FROM %s where blurb:'blurb1?'", Person.class.getName());
   private String allPersonsQuery = "FROM " + Person.class.getName();

   public Object[] factory() {
      return new Object[]{
            new ClusteredQueryTest().storageType(StorageType.OFF_HEAP),
            new ClusteredQueryTest().storageType(StorageType.BINARY),
            new ClusteredQueryTest().storageType(StorageType.OBJECT),
      };
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "StorageType");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), storageType);
   }

   ClusteredQueryTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
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
            .indexing().enable()
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      cacheCfg.memory()
            .storageType(storageType);
      List<Cache<String, Person>> caches = createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);
      cacheAMachine1 = caches.get(0);
      cacheAMachine2 = caches.get(1);
      populateCache();
   }

   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
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

   public void testLazyOrdered() {
      CacheQuery<Person> cacheQuery = createSortedQuery("age");

      for (int i = 0; i < 2; i++) {
         try (ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY))) {
            assert cacheQuery.getResultSize() == 10 : cacheQuery.getResultSize();

            int previousAge = 0;
            while (iterator.hasNext()) {
               Person person = iterator.next();
               assert person.getAge() > previousAge;
               previousAge = person.getAge();
            }
         }
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testLazyNonOrdered() {
      try (ResultIterator<Person> ignored = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY))) {
         assert cacheQuery.getResultSize() == 10 : cacheQuery.getResultSize();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testLocalQuery() {
      final SearchManager searchManager1 = Search.getSearchManager(cacheAMachine1);
      final CacheQuery<Person> localQuery1 = searchManager1.getQuery(queryString, IndexedQueryMode.FETCH);
      List<Person> results1 = localQuery1.list();

      final SearchManager searchManager2 = Search.getSearchManager(cacheAMachine2);
      final CacheQuery<Person> localQuery2 = searchManager2.getQuery(queryString, IndexedQueryMode.FETCH);
      List<Person> results2 = localQuery2.list();

      assertEquals(10, results1.size());
      assertEquals(10, results2.size());

      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testEagerOrdered() {
      CacheQuery<Person> cacheQuery = createSortedQuery("age");

      try (ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER))) {
         assertEquals(10, cacheQuery.getResultSize());

         int previousAge = 0;
         while (iterator.hasNext()) {
            Person person = iterator.next();
            assert person.getAge() > previousAge;
            previousAge = person.getAge();
         }
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   @Test(expectedExceptions = NoSuchElementException.class)
   public void testIteratorNextOutOfBounds() {
      cacheQuery.maxResults(1);
      try (ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER))) {
         assert iterator.hasNext();
         iterator.next();

         assert !iterator.hasNext();
         iterator.next();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testIteratorRemove() {
      cacheQuery.maxResults(1);
      try (ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER))) {
         assert iterator.hasNext();
         iterator.remove();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testList() {
      CacheQuery<Person> cacheQuery = createSortedQuery("age");

      List<Person> results = cacheQuery.list();
      assertEquals(10, cacheQuery.getResultSize());

      int previousAge = 0;
      for (Person person : results) {
         assert person.getAge() > previousAge;
         previousAge = person.getAge();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testGetResultSizeList() {
      assertEquals(10, cacheQuery.getResultSize());
   }

   public void testPagination() {
      CacheQuery<Person> cacheQuery = createSortedQuery("age");
      cacheQuery.firstResult(2);
      cacheQuery.maxResults(1);

      List<Person> results = cacheQuery.list();
      assertEquals(1, results.size());
      assertEquals(10, cacheQuery.getResultSize());
      Person result = results.get(0);
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
      CacheQuery<Person> paginationQuery = buildPaginationQuery(0, pageSize, sortField);

      int idx = 0;
      Set<String> keys = new HashSet<>();
      while (idx < NUM_ENTRIES) {
         List<Person> results = paginationQuery.list();
         results.stream().map(Person::getName).forEach(keys::add);
         idx += pageSize;
         paginationQuery = buildPaginationQuery(idx, pageSize, sortField);
      }

      assertEquals(NUM_ENTRIES, keys.size());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   private CacheQuery<Person> buildPaginationQuery(int offset, int pageSize, String sortField) {
      String sortedQuery = sortField != null ? allPersonsQuery + " ORDER BY " + sortField : allPersonsQuery;
      CacheQuery<Person> clusteredQuery = Search.getSearchManager(cacheAMachine1)
            .getQuery(sortedQuery, IndexedQueryMode.BROADCAST);
      clusteredQuery.firstResult(offset);
      clusteredQuery.maxResults(pageSize);
      return clusteredQuery;
   }

   public void testQueryAll() {
      CacheQuery<Person> clusteredQuery = Search.getSearchManager(cacheAMachine1)
            .getQuery(allPersonsQuery, IndexedQueryMode.BROADCAST);

      assertEquals(NUM_ENTRIES, clusteredQuery.list().size());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testFuzzyQuery() {
      populateCache();

      String q = String.format("FROM %s WHERE name:'name1'~2", Person.class.getName());

      CacheQuery<Person> clusteredQuery = Search.getSearchManager(cacheAMachine1)
            .getQuery(q, IndexedQueryMode.BROADCAST);

      assertEquals(NUM_ENTRIES, clusteredQuery.list().size());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastIckleMatchAllQuery() {
      CacheQuery<Person> everybody = Search.getSearchManager(cacheAMachine1)
            .getQuery(String.format("FROM %s", Person.class.getName()), IndexedQueryMode.BROADCAST);

      assertEquals(NUM_ENTRIES, everybody.list().size());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastIckleTermQuery() {
      String targetName = "name2";
      CacheQuery<Person> singlePerson = Search.getSearchManager(cacheAMachine1)
            .getQuery(String.format("FROM %s p where p.name:'%s'", Person.class.getName(), targetName),
                  IndexedQueryMode.BROADCAST);

      assertEquals(1, singlePerson.list().size());
      assertEquals(targetName, singlePerson.list().iterator().next().getName());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastFuzzyIckle() {
      CacheQuery<Person> persons0to10 = Search.getSearchManager(cacheAMachine1)
            .getQuery(String.format("FROM %s p where p.name:'nome'~2", Person.class.getName()),
                  IndexedQueryMode.BROADCAST);

      assertEquals(10, persons0to10.list().size());

      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastNumericRangeQuery() {
      CacheQuery<Person> infants = Search.getSearchManager(cacheAMachine1)
            .getQuery(String.format("FROM %s p where p.age between 0 and 5", Person.class.getName()),
                  IndexedQueryMode.BROADCAST);

      assertEquals(6, infants.list().size());

      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastProjectionIckleQuery() {
      SearchManager sm = Search.getSearchManager(cacheAMachine2);
      CacheQuery<Object[]> onlyNames = sm.getQuery(
            String.format("Select p.name FROM %s p", Person.class.getName()), IndexedQueryMode.BROADCAST
      );

      List<Object[]> results = onlyNames.list();
      assertEquals(NUM_ENTRIES, results.size());

      Set<String> names = new HashSet<>();
      results.iterator().forEachRemaining(s -> names.add((String) s[0]));

      Set<String> allNames = IntStream.range(0, NUM_ENTRIES).boxed().map(i -> "name" + i).collect(Collectors.toSet());
      assertEquals(allNames, names);
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testBroadcastSortedIckleQuery() {
      SearchManager sm = Search.getSearchManager(cacheAMachine2);
      CacheQuery<Person> theLastWillBeFirst = sm.getQuery(
            String.format("FROM %s p order by p.age desc", Person.class.getName()), IndexedQueryMode.BROADCAST);

      List<Person> results = theLastWillBeFirst.list();
      assertEquals(NUM_ENTRIES, results.size());
      assertEquals(NUM_ENTRIES - 1, results.iterator().next().getAge());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testPaginatedIckleQuery() {
      SearchManager sm = Search.getSearchManager(cacheAMachine1);
      CacheQuery<Person> q = sm.getQuery(String.format("FROM %s p order by p.age", Person.class.getName()),
            IndexedQueryMode.BROADCAST);

      q.firstResult(5);
      q.maxResults(10);

      List<Person> results = q.list();

      assertEquals(10, results.size());
      assertEquals("name5", results.iterator().next().getName());
      assertEquals("name14", results.get(9).getName());
   }

   public void testPartialIckleQuery() {
      SearchManager searchManager1 = Search.getSearchManager(cacheAMachine1);
      SearchManager searchManager2 = Search.getSearchManager(cacheAMachine2);
      String query = String.format("FROM %s p", Person.class.getName());

      CacheQuery<Person> machine1Results = searchManager1.getQuery(query, IndexedQueryMode.FETCH);
      CacheQuery<Person> machine2Results = searchManager2.getQuery(query, IndexedQueryMode.FETCH);

      int numDocsMachine1 = countLocalIndex(cacheAMachine1);
      int numDocsMachine2 = countLocalIndex(cacheAMachine2);

      assertEquals(2 * NUM_ENTRIES, numDocsMachine1 + numDocsMachine2);
      assertEquals(numDocsMachine1, machine1Results.list().size());
      assertEquals(numDocsMachine2, machine2Results.list().size());

      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   private int countLocalIndex(Cache<String, Person> cache) {
      SearchManager sm = Search.getSearchManager(cache);
      ExtendedSearchIntegrator esi = sm.unwrap(ExtendedSearchIntegrator.class);
      IndexReader indexReader = esi.getIndexManager("person").getReaderProvider().openIndexReader();
      return indexReader.numDocs();
   }

   @Test(expectedExceptions = SearchException.class, expectedExceptionsMessageRegExp = ".*cannot be converted to an indexed query")
   public void testPreventHybridQuery() {
      CacheQuery<Person> hybridQuery = Search.getSearchManager(cacheAMachine1)
            .getQuery(String.format("FROM %s p where p.nonIndexedField = 'nothing'", Person.class.getName()),
                  IndexedQueryMode.BROADCAST);

      hybridQuery.list();
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = ".*cannot be converted to an indexed query")
   public void testPreventAggregationQueries() {
      CacheQuery<Person> aggregationQuery = Search.getSearchManager(cacheAMachine1)
            .getQuery(String.format("FROM %s p where p.name:'name3' group by p.name", Person.class.getName()),
                  IndexedQueryMode.BROADCAST);

      aggregationQuery.list();
   }

   @Test
   public void testBroadcastNativeInfinispanMatchAllQuery() {
      String q = String.format("FROM %s", Person.class.getName());
      Query partialResultQuery = Search.getQueryFactory(cacheAMachine1).create(q);
      Query fullResultQuery = Search.getQueryFactory(cacheAMachine2).create(q, IndexedQueryMode.BROADCAST);

      int docsInMachine1 = countLocalIndex(cacheAMachine1);

      assertEquals(docsInMachine1, partialResultQuery.list().size());
      assertEquals(NUM_ENTRIES, fullResultQuery.list().size());
   }

   @Test
   public void testBroadcastNativeInfinispanHybridQuery() {
      String q = "FROM " + Person.class.getName() + " where age >= 40 and nonIndexedField = 'na'";
      Query query = Search.getQueryFactory(cacheAMachine1).create(q, IndexedQueryMode.BROADCAST);

      assertEquals(10, query.list().size());
   }

   @Test
   public void testBroadcastNativeInfinispanFuzzyQuery() {
      String q = String.format("FROM %s p where p.name:'nome'~2", Person.class.getName());

      Query query = Search.getQueryFactory(cacheAMachine1).create(q, IndexedQueryMode.BROADCAST);

      assertEquals(10, query.list().size());
   }

   @Test
   public void testBroadcastSortedInfinispanQuery() {
      QueryFactory queryFactory = Search.getQueryFactory(cacheAMachine1);
      Query sortedQuery = queryFactory.create("FROM " + Person.class.getName() + " p order by p.age desc",
            IndexedQueryMode.BROADCAST);

      List<Person> results = sortedQuery.list();
      assertEquals(NUM_ENTRIES, results.size());
      assertEquals(NUM_ENTRIES - 1, results.iterator().next().getAge());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   @Test
   public void testBroadcastAggregatedInfinispanQuery() {
      QueryFactory queryFactory = Search.getQueryFactory(cacheAMachine2);
      Query hybridQuery = queryFactory.create("select name FROM " + Person.class.getName() + " WHERE name : 'na*' group by name",
            IndexedQueryMode.BROADCAST);

      assertEquals(NUM_ENTRIES, hybridQuery.list().size());
   }

   @Test
   public void testNonIndexedBroadcastInfinispanQuery() {
      QueryFactory queryFactory = Search.getQueryFactory(cacheAMachine2);
      Query slowQuery = queryFactory.create("FROM " + Person.class.getName() + " WHERE nonIndexedField LIKE 'na%'",
            IndexedQueryMode.BROADCAST);

      assertEquals(NUM_ENTRIES, slowQuery.list().size());
   }

   protected void populateCache() {
      prepareTestData();

      cacheQuery = Search.getSearchManager(cacheAMachine1).getQuery(queryString, IndexedQueryMode.BROADCAST);
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   protected CacheQuery<Person> createSortedQuery(String sortField) {
      String q = String.format("FROM %s p where p.blurb:'blurb1?' order by p.%s'", Person.class.getName(), sortField);
      return Search.getSearchManager(cacheAMachine1).getQuery(q, IndexedQueryMode.BROADCAST);
   }
}
