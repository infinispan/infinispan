package org.infinispan.query.blackbox;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;
import static org.testng.AssertJUnit.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * ClusteredQueryTest.
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredQueryTest")
public class ClusteredQueryTest extends MultipleCacheManagersTest {

   private final QueryParser queryParser = createQueryParser("blurb");

   static final int NUM_ENTRIES = 30;

   Cache<String, Person> cacheAMachine1, cacheAMachine2;
   CacheQuery<Person> cacheQuery;
   protected StorageType storageType;

   public ClusteredQueryTest() {
      // BasicConfigurator.configure();
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public Object[] factory() {
      return new Object[]{
            new ClusteredQueryTest().storageType(StorageType.OFF_HEAP),
            new ClusteredQueryTest().storageType(StorageType.BINARY),
            new ClusteredQueryTest().storageType(StorageType.OBJECT),
      };
   }

   ClusteredQueryTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   protected void enhanceConfig(ConfigurationBuilder cacheCfg) {
      // meant to be overridden
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(getCacheMode(), false);
      cacheCfg
            .indexing()
            .index(Index.PRIMARY_OWNER)
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      cacheCfg.memory()
            .storageType(storageType);
      enhanceConfig(cacheCfg);
      List<Cache<String, Person>> caches = createClusteredCaches(2, cacheCfg);
      cacheAMachine1 = caches.get(0);
      cacheAMachine2 = caches.get(1);
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

   public void testLazyOrdered() throws ParseException {
      populateCache();

      // applying sort
      SortField sortField = new SortField("age", Type.INT);
      Sort sort = new Sort(sortField);
      cacheQuery.sort(sort);

      for (int i = 0; i < 2; i++) {
         ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));
         try {
            assert cacheQuery.getResultSize() == 10 : cacheQuery.getResultSize();

            int previousAge = 0;
            while (iterator.hasNext()) {
               Person person = iterator.next();
               assert person.getAge() > previousAge;
               previousAge = person.getAge();
            }
         } finally {
            iterator.close();
         }
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testLazyNonOrdered() throws ParseException {
      populateCache();

      ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));
      try {
         assert cacheQuery.getResultSize() == 10 : cacheQuery.getResultSize();
      } finally {
         iterator.close();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testLocalQuery() throws ParseException {
      populateCache();

      final SearchManager searchManager1 = Search.getSearchManager(cacheAMachine1);
      final CacheQuery<Person> localQuery1 = searchManager1.getQuery(createLuceneQuery());
      List<Person> results1 =  localQuery1.list();

      final SearchManager searchManager2 = Search.getSearchManager(cacheAMachine2);
      final CacheQuery<Person> localQuery2 = searchManager2.getQuery(createLuceneQuery());
      List<Person> results2 =  localQuery2.list();

      assertEquals(10, results1.size() + results2.size());

      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testEagerOrdered() throws ParseException {
      populateCache();

      // applying sort
      SortField sortField = new SortField("age", Type.INT);
      Sort sort = new Sort(sortField);
      cacheQuery.sort(sort);

      ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));
      try {
         assertEquals(10, cacheQuery.getResultSize());

         int previousAge = 0;
         while (iterator.hasNext()) {
            Person person = iterator.next();
            assert person.getAge() > previousAge;
            previousAge = person.getAge();
         }
      } finally {
         iterator.close();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   @Test(expectedExceptions = NoSuchElementException.class, expectedExceptionsMessageRegExp = "Out of boundaries")
   public void testIteratorNextOutOfBounds() throws Exception {
      populateCache();

      cacheQuery.maxResults(1);
      ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));
      try {
         assert iterator.hasNext();
         iterator.next();

         assert !iterator.hasNext();
         iterator.next();
      } finally {
         iterator.close();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   public void testIteratorRemove() throws Exception {
      populateCache();

      cacheQuery.maxResults(1);
      ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));
      try {
         assert iterator.hasNext();
         iterator.remove();
      } finally {
         iterator.close();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testList() throws ParseException {
      populateCache();

      // applying sort
      SortField sortField = new SortField("age", Type.INT);
      Sort sort = new Sort(sortField);
      cacheQuery.sort(sort);

      List<Person> results = cacheQuery.list();
      assertEquals(10, cacheQuery.getResultSize());

      int previousAge = 0;
      for (Person person : results) {
         assert person.getAge() > previousAge;
         previousAge = person.getAge();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testGetResultSizeList() throws ParseException {
      populateCache();
      assertEquals(10, cacheQuery.getResultSize());
   }

   public void testPagination() throws ParseException {
      populateCache();

      cacheQuery.firstResult(2);
      cacheQuery.maxResults(1);

      // applying sort
      SortField sortField = new SortField("age", Type.INT);
      Sort sort = new Sort(sortField);
      cacheQuery.sort(sort);

      List<Person> results = cacheQuery.list();
      assertEquals(1, results.size());
      assertEquals(10, cacheQuery.getResultSize());
      Person result = results.get(0);
      assertEquals(12, result.getAge());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   @Test
   public void testPagination2() throws Exception {
      int[] pageSizes = new int[]{1, 5, 7, NUM_ENTRIES + 10};

      for (int pageSize : pageSizes) {
         testPaginationWithoutSort(pageSize);
         testPaginationWithSort(pageSize, "age", Type.INT);
      }
   }

   private void testPaginationWithoutSort(int pageSize) throws ParseException {
      testPaginationInternal(pageSize, null);
   }

   private void testPaginationWithSort(int pageSize, String field, Type type) throws ParseException {
      testPaginationInternal(pageSize, new Sort(new SortField(field, type)));
   }

   private void testPaginationInternal(int pageSize, Sort sort) throws ParseException {
      populateCache();

      CacheQuery<Person> paginationQuery = buildPaginationQuery(0, pageSize, sort);

      int idx = 0;
      Set<String> keys = new HashSet<>();
      while (idx < NUM_ENTRIES) {
         List<Person> results = paginationQuery.list();
         results.stream().map(Person::getName).forEach(keys::add);
         idx += pageSize;
         paginationQuery = buildPaginationQuery(idx, pageSize, sort);
      }

      assertEquals(NUM_ENTRIES, keys.size());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   private CacheQuery<Person> buildPaginationQuery(int offset, int pageSize, Sort sort) throws ParseException {
      CacheQuery<Person> clusteredQuery = Search.getSearchManager(cacheAMachine1)
            .getClusteredQuery(new MatchAllDocsQuery());
      clusteredQuery.firstResult(offset);
      clusteredQuery.maxResults(pageSize);
      if (sort != null) {
         clusteredQuery.sort(sort);
      }
      return clusteredQuery;
   }

   public void testQueryAll() throws ParseException {
      populateCache();
      CacheQuery<Person> clusteredQuery = Search.getSearchManager(cacheAMachine1)
            .getClusteredQuery(new MatchAllDocsQuery(), Person.class);

      assertEquals(NUM_ENTRIES, clusteredQuery.list().size());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   protected void populateCache() throws ParseException {
      prepareTestData();

      cacheQuery = Search.getSearchManager(cacheAMachine1).getClusteredQuery(createLuceneQuery());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   protected BooleanQuery createLuceneQuery() throws ParseException {
      return new BooleanQuery.Builder()
            .add(queryParser.parse("blurb1?"), Occur.SHOULD)
            .build();
   }

}
