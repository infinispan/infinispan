package org.infinispan.query.blackbox;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.NoSuchElementException;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
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
 *
 * @author Israel Lacerra <israeldl@gmail.com>
 * @since 5.1
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredQueryTest")
public class ClusteredQueryTest extends MultipleCacheManagersTest {

   private final QueryParser queryParser = createQueryParser("blurb");

   Cache<String, Person> cacheAMachine1, cacheAMachine2;
   CacheQuery<Person> cacheQuery;

   public ClusteredQueryTest() {
      // BasicConfigurator.configure();
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected void enhanceConfig(ConfigurationBuilder cacheCfg) {
      // meant to be overridden
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(getCacheMode(), false);
      cacheCfg
         .indexing()
            .index(Index.LOCAL)
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "ram")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cacheCfg);
      List<Cache<String, Person>> caches = createClusteredCaches(2, cacheCfg);
      cacheAMachine1 = caches.get(0);
      cacheAMachine2 = caches.get(1);
   }

   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   protected void prepareTestData() {
      Person person1 = new Person();
      person1.setName("NavinSurtani");
      person1.setBlurb("Likes playing WoW");
      person1.setAge(45);

      Person person2 = new Person();
      person2.setName("BigGoat");
      person2.setBlurb("Eats grass");
      person2.setAge(30);

      Person person3 = new Person();
      person3.setName("MiniGoat");
      person3.setBlurb("Eats cheese");
      person3.setAge(35);

      // Put the 3 created objects in the cache1.

      cacheAMachine2.put("Navin", person1);
      cacheAMachine1.put("BigGoat", person2);
      cacheAMachine1.put("MiniGoat", person3);

      Person person4 = new Person();
      person4.setName("MightyGoat");
      person4.setBlurb("Also eats grass");
      person4.setAge(66);

      cacheAMachine1.put("newOne", person4);
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testLazyOrdered() throws ParseException {
      populateCache();

      // applying sort
      SortField sortField = new SortField("age", SortField.Type.INT);
      Sort sort = new Sort(sortField);
      cacheQuery.sort(sort);

      for (int i = 0; i < 2; i ++) {
         ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));
         try {
            assert cacheQuery.getResultSize() == 4 : cacheQuery.getResultSize();

            int previousAge = 0;
            while (iterator.hasNext()) {
               Person person = iterator.next();
               assert person.getAge() > previousAge;
               previousAge = person.getAge();
            }
         }
         finally {
            iterator.close();
         }
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testLazyNonOrdered() throws ParseException {
      populateCache();

      ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY));
      try {
         assert cacheQuery.getResultSize() == 4 : cacheQuery.getResultSize();
      }
      finally {
         iterator.close();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testLocalQuery() throws ParseException {
      populateCache();

      final SearchManager searchManager1 = Search.getSearchManager(cacheAMachine1);
      final CacheQuery<?> localQuery1 = searchManager1.getQuery(createLuceneQuery());
      assertEquals(3, localQuery1.getResultSize());

      final SearchManager searchManager2 = Search.getSearchManager(cacheAMachine2);
      final CacheQuery<?> localQuery2 = searchManager2.getQuery(createLuceneQuery());
      assertEquals(1, localQuery2.getResultSize());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testEagerOrdered() throws ParseException {
      populateCache();

      // applying sort
      SortField sortField = new SortField("age", SortField.Type.INT);
      Sort sort = new Sort(sortField);
      cacheQuery.sort(sort);

      ResultIterator<Person> iterator = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.EAGER));
      try {
         assertEquals(4, cacheQuery.getResultSize());

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
      SortField sortField = new SortField("age", SortField.Type.INT);
      Sort sort = new Sort(sortField);
      cacheQuery.sort(sort);

      List<Person> results = cacheQuery.list();
      assert results.size() == 4 : cacheQuery.getResultSize();

      int previousAge = 0;
      for (Person person : results) {
         assert person.getAge() > previousAge;
         previousAge = person.getAge();
      }
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testGetResultSizeList() throws ParseException {
      populateCache();
      assertEquals(4, cacheQuery.getResultSize());
   }

   public void testPagination() throws ParseException {
      populateCache();

      cacheQuery.firstResult(2);
      cacheQuery.maxResults(1);

      // applying sort
      SortField sortField = new SortField("age", SortField.Type.INT);
      Sort sort = new Sort(sortField);
      cacheQuery.sort(sort);

      List<Person> results = cacheQuery.list();
      assertEquals(1, results.size());
      assertEquals(4, cacheQuery.getResultSize());
      Person result = results.get(0);
      assertEquals(45, result.getAge());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   public void testQueryAll() throws ParseException {
      populateCache();
      CacheQuery<Person> clusteredQuery = Search.getSearchManager(cacheAMachine1)
            .getClusteredQuery(new MatchAllDocsQuery(), Person.class);

      assertEquals(4, clusteredQuery.list().size());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   private void populateCache() throws ParseException {
      prepareTestData();

      cacheQuery = Search.getSearchManager(cacheAMachine1).getClusteredQuery(createLuceneQuery());
      StaticTestingErrorHandler.assertAllGood(cacheAMachine1, cacheAMachine2);
   }

   private BooleanQuery createLuceneQuery() throws ParseException {
      return new BooleanQuery.Builder()
              .add(queryParser.parse("eats"), Occur.SHOULD)
              .add(queryParser.parse("playing"), Occur.SHOULD)
              .build();
   }

}
