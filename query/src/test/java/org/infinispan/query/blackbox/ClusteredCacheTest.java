package org.infinispan.query.blackbox;

import static org.infinispan.distribution.Ownership.BACKUP;
import static org.infinispan.distribution.Ownership.NON_OWNER;
import static org.infinispan.distribution.Ownership.PRIMARY;
import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.transaction.TransactionManager;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.hibernate.search.filter.FullTextFilter;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.hibernate.search.query.engine.spi.HSQuery;
import org.hibernate.search.spi.SearchIntegrator;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.Ownership;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.spi.SearchManagerImplementor;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author Navin Surtani
 * @author Sanne Grinovero
 */
@Test(groups = {"functional"}, testName = "query.blackbox.ClusteredCacheTest")
public class ClusteredCacheTest extends MultipleCacheManagersTest {

   protected Cache<Object, Person> cache1;
   protected Cache<Object, Person> cache2;
   protected StorageType storageType;
   private Person person1;
   private Person person2;
   private Person person3;
   private Person person4;
   private QueryParser queryParser;
   private Query luceneQuery;
   private CacheQuery<Person> cacheQuery;
   private final String key1 = "Navin";
   private final String key2 = "BigGoat";
   private final String key3 = "MiniGoat";

   public ClusteredCacheTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "StorageType");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), storageType);
   }

   public Object[] factory() {
      return new Object[]{
            new ClusteredCacheTest().storageType(StorageType.OFF_HEAP),
            new ClusteredCacheTest().storageType(StorageType.BINARY),
            new ClusteredCacheTest().storageType(StorageType.OBJECT),
      };
   }

   ClusteredCacheTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   protected void enhanceConfig(ConfigurationBuilder cacheCfg) {
      // meant to be overridden
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg.indexing()
            .index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      cacheCfg.memory()
            .storageType(storageType);
      enhanceConfig(cacheCfg);
      List<Cache<Object, Person>> caches = createClusteredCaches(2, cacheCfg);
      cache1 = caches.get(0);
      cache2 = caches.get(1);
   }

   private void prepareTestedObjects() {
      person1 = new Person();
      person1.setName("Navin Surtani");
      person1.setBlurb("Likes playing WoW");
      person1.setAge(30);

      person2 = new Person();
      person2.setName("BigGoat");
      person2.setBlurb("Eats grass");
      person2.setAge(22);

      person3 = new Person();
      person3.setName("MiniGoat");
      person3.setBlurb("Eats cheese");
      person3.setAge(15);
   }

   protected void prepareTestData() throws Exception {
      prepareTestedObjects();

      TransactionManager transactionManager = cache1.getAdvancedCache().getTransactionManager();

      // Put the 3 created objects in the cache1.

      if (transactionsEnabled()) transactionManager.begin();
      cache1.put(key1, person1);
      cache1.put(key2, person2);
      cache1.put(key3, person3);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   protected boolean transactionsEnabled() {
      return false;
   }

   public void testSimple() throws Exception {
      prepareTestData();
      cacheQuery = createCacheQuery("blurb:playing");

      List<Person> found = cacheQuery.list();

      assertEquals(1, found.size());

      if (found.get(0) == null) {
         log.warn("found.get(0) is null");
         Person p1 = cache2.get(key1);
         if (p1 == null) {
            log.warn("Person p1 is null in sc2 and cannot actually see the data of person1 in sc1");
         } else {
            log.trace("p1 name is  " + p1.getName());
         }
      }

      assertEquals(person1, found.get(0));
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   private void assertQueryInterceptorPresent(Cache<?, ?> c) {
      QueryInterceptor i = TestingUtil.findInterceptor(c, QueryInterceptor.class);
      assert i != null : "Expected to find a QueryInterceptor, only found " +
            c.getAdvancedCache().getAsyncInterceptorChain().getInterceptors();
   }

   public void testModified() throws Exception {
      prepareTestData();
      assertQueryInterceptorPresent(cache2);

      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("playing");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);

      List<Person> found = cacheQuery.list();

      assert found.size() == 1 : "Expected list of size 1, was of size " + found.size();
      assert found.get(0).equals(person1);

      person1.setBlurb("Likes pizza");
      cache1.put("Navin", person1);

      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("pizza");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);

      found = cacheQuery.list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testAdded() throws Exception {
      prepareTestData();
      queryParser = createQueryParser("blurb");

      luceneQuery = queryParser.parse("eats");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);
      List<Person> found = cacheQuery.list();

      assertEquals(2, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));
      assertFalse("This should not contain object person4", found.contains(person4));

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");

      cache1.put("mighty", person4);

      luceneQuery = queryParser.parse("eats");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);
      found = cacheQuery.list();

      assertEquals(3, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));
      assertTrue("This should now contain object person4", found.contains(person4));
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testRemoved() throws Exception {
      prepareTestData();
      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("eats");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);
      List<Person> found = cacheQuery.list();

      assert found.size() == 2;
      assert found.contains(person2);
      assert found.contains(person3) : "This should still contain object person3";

      cache1.remove(key3);

      found = cacheQuery.list();
      assert found.size() == 1;
      assert found.contains(person2);
      assert !found.contains(person3) : "This should not contain object person3 anymore";
      assert countIndex(cache1) == 2 : "Two documents should remain in the index";
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   protected int queryIndex(Cache<?, ?> cache, String query) throws ParseException {
      QueryParser qp = createQueryParser("blurb");
      Query q = qp.parse(query);
      HSQuery hsQuery = Search.getSearchManager(cache).unwrap(SearchIntegrator.class).createHSQuery(q, Person.class);
      return hsQuery.queryResultSize();
   }

   protected int countIndex(Cache<?, ?> cache) throws ParseException {
      return queryIndex(cache, "*:*");
   }

   private Optional<Cache<Object, Person>> findCache(Ownership ownership, Object key) {
      List<Cache<Object, Person>> caches = caches();
      ClusteringDependentLogic cdl = cache1.getAdvancedCache().getComponentRegistry().getComponent(ClusteringDependentLogic.class);
      DistributionInfo distribution = cdl.getCacheTopology().getDistribution(key);

      Predicate<Cache<?, ?>> predicate = null;
      switch (ownership) {
         case PRIMARY:
            predicate = c -> c.getAdvancedCache().getRpcManager().getAddress().equals(distribution.primary());
            break;
         case BACKUP:
            predicate = c -> distribution.writeBackups().contains(c.getAdvancedCache().getRpcManager().getAddress());
            break;
         case NON_OWNER:
            predicate = c -> !distribution.writeOwners().contains(c.getAdvancedCache().getRpcManager().getAddress());
      }

      return caches.stream().filter(predicate).findFirst();
   }


   public void testConditionalRemoveFromPrimary() throws Exception {
      testConditionalRemoveFrom(PRIMARY);
   }

   public void testConditionalRemoveFromBackup() throws Exception {
      testConditionalRemoveFrom(BACKUP);
   }

   public void testConditionalRemoveFromNonOwner() throws Exception {
      testConditionalRemoveFrom(NON_OWNER);
   }

   public void testConditionalReplaceFromPrimary() throws Exception {
      testConditionalReplaceFrom(PRIMARY);
   }

   public void testConditionalReplaceFromBackup() throws Exception {
      testConditionalReplaceFrom(BACKUP);
   }

   public void testConditionalReplaceFromNonOwner() throws Exception {
      testConditionalReplaceFrom(NON_OWNER);
   }

   private <T> CacheQuery<T> createCacheQuery(String query) throws ParseException {
      queryParser = createQueryParser(query.substring(0, query.indexOf(':')));
      Query q = queryParser.parse(query);
      return Search.getSearchManager(cache1).getQuery(q);
   }

   private void testConditionalReplaceFrom(Ownership memberType) throws Exception {
      prepareTestData();

      Cache<Object, Person> cache = findCache(memberType, key1).orElse(cache2);

      assertEquals(createCacheQuery("blurb:wow").list().size(), 1);

      boolean replaced = cache.replace(key1, person1, person2);

      assertTrue(replaced);
      assertEquals(createCacheQuery("blurb:wow").list().size(), 0);
      assertEquals(queryIndex(cache, "blurb:wow"), 0);
   }

   private void testConditionalRemoveFrom(Ownership owneship) throws Exception {
      prepareTestData();

      CacheQuery<Object> query = Search.getSearchManager(cache2).getQuery(new MatchAllDocsQuery());

      Cache<Object, Person> cache = findCache(owneship, key1).orElse(cache2);

      cache.remove(key1, person1);

      assertEquals(query.list().size(), 2);
      assertEquals(countIndex(cache), 2);

      cache.remove(key1, person1);

      assertEquals(query.list().size(), 2);
      assertEquals(countIndex(cache), 2);

      cache = findCache(owneship, key3).orElse(cache2);

      cache.remove(key3);

      assertEquals(query.list().size(), 1);
      assertEquals(countIndex(cache), 1);

      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testGetResultSize() throws Exception {
      prepareTestData();
      queryParser = createQueryParser("blurb");
      luceneQuery = queryParser.parse("playing");
      cacheQuery = Search.getSearchManager(cache2).getQuery(luceneQuery);
      List<Person> found = cacheQuery.list();

      assertEquals(1, found.size());
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutMap() throws Exception {
      prepareTestData();
      SearchManager searchManager = Search.getSearchManager(cache2);
      QueryBuilder queryBuilder = searchManager
            .buildQueryBuilderForClass(Person.class)
            .get();
      Query allQuery = queryBuilder.all().createQuery();
      assertEquals(3, searchManager.getQuery(allQuery, Person.class).list().size());

      Map<String, Person> allWrites = new HashMap<>();
      allWrites.put(key1, person1);
      allWrites.put(key2, person2);
      allWrites.put(key3, person3);

      cache2.putAll(allWrites);
      List<Person> found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(3, found.size());

      cache2.putAll(allWrites);
      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(3, found.size());
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutMapAsync() throws Exception {
      prepareTestData();
      SearchManager searchManager = Search.getSearchManager(cache2);
      QueryBuilder queryBuilder = searchManager
            .buildQueryBuilderForClass(Person.class)
            .get();
      Query allQuery = queryBuilder.all().createQuery();
      assert searchManager.getQuery(allQuery, Person.class).list().size() == 3;

      person4 = new Person();
      person4.setName("New Goat");
      person4.setBlurb("Also eats grass");

      Map<String, Person> allWrites = new HashMap<>();
      allWrites.put(key1, person1);
      allWrites.put(key2, person2);
      allWrites.put(key3, person3);
      allWrites.put("newGoat", person4);

      Future futureTask = cache2.putAllAsync(allWrites);
      futureTask.get();
      assert futureTask.isDone();
      List<Person> found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assert found.contains(person4);

      futureTask = cache1.putAllAsync(allWrites);
      futureTask.get();
      assert futureTask.isDone();
      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assert found.contains(person4);
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutForExternalRead() throws Exception {
      prepareTestData();
      SearchManager searchManager = Search.getSearchManager(cache2);
      QueryBuilder queryBuilder = searchManager
            .buildQueryBuilderForClass(Person.class)
            .get();
      Query allQuery = queryBuilder.all().createQuery();
      assertEquals(3, searchManager.getQuery(allQuery, Person.class).list().size());

      person4 = new Person();
      person4.setName("New Goat");
      person4.setBlurb("Also eats grass");

      cache2.putForExternalRead("newGoat", person4);
      eventually(() -> cache2.get("newGoat") != null);
      List<Person> found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());

      assertTrue(found.contains(person4));

      Person person5 = new Person();
      person5.setName("Abnormal Goat");
      person5.setBlurb("Plays with grass.");
      cache2.putForExternalRead("newGoat", person5);

      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());

      assertFalse(found.contains(person5));
      assertTrue(found.contains(person4));
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutIfAbsent() throws Exception {
      prepareTestData();
      SearchManager searchManager = Search.getSearchManager(cache2);
      QueryBuilder queryBuilder = searchManager
            .buildQueryBuilderForClass(Person.class)
            .get();
      Query allQuery = queryBuilder.all().createQuery();
      assert searchManager.getQuery(allQuery, Person.class).list().size() == 3;

      person4 = new Person();
      person4.setName("New Goat");
      person4.setBlurb("Also eats grass");

      cache2.putIfAbsent("newGoat", person4);

      List<Person> found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());

      assert found.contains(person4);

      Person person5 = new Person();
      person5.setName("Abnormal Goat");
      person5.setBlurb("Plays with grass.");
      cache2.putIfAbsent("newGoat", person5);

      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());

      assertFalse(found.contains(person5));
      assertTrue(found.contains(person4));
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutIfAbsentAsync() throws Exception {
      prepareTestData();
      SearchManager searchManager = Search.getSearchManager(cache2);
      QueryBuilder queryBuilder = searchManager
            .buildQueryBuilderForClass(Person.class)
            .get();
      Query allQuery = queryBuilder.all().createQuery();
      assertEquals(3, searchManager.getQuery(allQuery, Person.class).list().size());

      person4 = new Person();
      person4.setName("New Goat");
      person4.setBlurb("Also eats grass");

      Future futureTask = cache2.putIfAbsentAsync("newGoat", person4);
      futureTask.get();
      assertTrue(futureTask.isDone());
      List<Person> found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assertTrue(found.contains(person4));

      Person person5 = new Person();
      person5.setName("Abnormal Goat");
      person5.setBlurb("Plays with grass.");
      futureTask = cache2.putIfAbsentAsync("newGoat", person5);
      futureTask.get();
      assertTrue(futureTask.isDone());
      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assertFalse(found.contains(person5));
      assertTrue(found.contains(person4));
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutAsync() throws Exception {
      prepareTestData();
      SearchManager searchManager = Search.getSearchManager(cache2);
      QueryBuilder queryBuilder = searchManager
            .buildQueryBuilderForClass(Person.class)
            .get();
      Query allQuery = queryBuilder.all().createQuery();
      assertEquals(3, searchManager.getQuery(allQuery, Person.class).list().size());

      person4 = new Person();
      person4.setName("New Goat");
      person4.setBlurb("Also eats grass");

      Future f = cache2.putAsync("newGoat", person4);
      f.get();
      assertTrue(f.isDone());
      List<Person> found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assertTrue(found.contains(person4));

      Person person5 = new Person();
      person5.setName("Abnormal Goat");
      person5.setBlurb("Plays with grass.");
      f = cache2.putAsync("newGoat", person5);
      f.get();
      assertTrue(f.isDone());
      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assertFalse(found.contains(person4));
      assertTrue(found.contains(person5));
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testClear() throws Exception {
      prepareTestData();
      queryParser = createQueryParser("blurb");

      BooleanQuery luceneQuery = new BooleanQuery.Builder()
            .add(queryParser.parse("eats"), Occur.SHOULD)
            .add(queryParser.parse("playing"), Occur.SHOULD)
            .build();
      CacheQuery<?> cacheQuery1 = Search.getSearchManager(cache1).getQuery(luceneQuery);
      CacheQuery<?> cacheQuery2 = Search.getSearchManager(cache2).getQuery(luceneQuery);

      assertEquals(3, cacheQuery1.getResultSize());
      assertEquals(3, cacheQuery2.getResultSize());

      cache2.clear();

      assertEquals(0, cacheQuery1.list().size());
      assertEquals(0, cacheQuery2.list().size());

      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testFullTextFilterOnOff() throws Exception {
      prepareTestData();
      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("eats");

      CacheQuery<Person> query = Search.getSearchManager(cache1).getQuery(luceneQuery);
      FullTextFilter filter = query.enableFullTextFilter("personFilter");
      filter.setParameter("blurbText", "cheese");

      assertEquals(1, query.getResultSize());
      List<Person> result = query.list();

      Person person = result.get(0);
      assertEquals("MiniGoat", person.getName());
      assertEquals("Eats cheese", person.getBlurb());

      //Disabling the fullTextFilter.
      query.disableFullTextFilter("personFilter");
      assertEquals(2, query.getResultSize());
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testCombinationOfFilters() throws Exception {
      prepareTestData();

      person4 = new Person();
      person4.setName("ExtraGoat");
      person4.setBlurb("Eats grass and is retired");
      person4.setAge(70);
      cache1.put("ExtraGoat", person4);

      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("eats");

      CacheQuery<Person> query = Search.getSearchManager(cache1).getQuery(luceneQuery);
      FullTextFilter filter = query.enableFullTextFilter("personFilter");
      filter.setParameter("blurbText", "grass");

      assertEquals(2, query.getResultSize());

      FullTextFilter ageFilter = query.enableFullTextFilter("personAgeFilter");
      ageFilter.setParameter("age", 70);

      assertEquals(1, query.getResultSize());
      List<Person> result = query.list();

      Person person = result.get(0);
      assertEquals("ExtraGoat", person.getName());
      assertEquals(70, person.getAge());

      //Disabling the fullTextFilter.
      query.disableFullTextFilter("personFilter");
      query.disableFullTextFilter("personAgeFilter");
      assertEquals(3, query.getResultSize());
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testSearchKeyTransformer() throws Exception {
      caches().forEach(cache -> {
         SearchManagerImplementor manager = (SearchManagerImplementor) Search.getSearchManager(cache);
         manager.registerKeyTransformer(CustomKey3.class, CustomKey3Transformer.class);
      });

      prepareTestedObjects();

      TransactionManager transactionManager = cache1.getAdvancedCache().getTransactionManager();

      CustomKey3 customKey1 = new CustomKey3(key1);
      CustomKey3 customKey2 = new CustomKey3(key2);
      CustomKey3 customKey3 = new CustomKey3(key3);

      // Put the 3 created objects in the cache1.
      if (transactionsEnabled()) transactionManager.begin();
      cache1.put(customKey1, person1);
      cache1.put(customKey2, person2);
      cache1.put(customKey3, person3);
      if (transactionsEnabled()) transactionManager.commit();

      queryParser = createQueryParser("blurb");
      Query luceneQuery = queryParser.parse("Eats");

      CacheQuery<Person> cacheQuery = Search.getSearchManager(cache1).getQuery(luceneQuery);

      int counter = 0;
      try (ResultIterator<Person> found = cacheQuery.iterator(new FetchOptions().fetchMode(FetchOptions.FetchMode.LAZY))) {
         while (found.hasNext()) {
            found.next();
            counter++;
         }
      }

      assertEquals(2, counter);
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testCompute() throws Exception {
      prepareTestData();
      TransactionManager transactionManager = cache2.getAdvancedCache().getTransactionManager();
      SearchManager searchManager = Search.getSearchManager(cache2);
      QueryBuilder queryBuilder = searchManager
            .buildQueryBuilderForClass(Person.class)
            .get();
      Query allQuery = queryBuilder.all().createQuery();
      assertEquals(3, searchManager.getQuery(allQuery, Person.class).list().size());

      String key = "newGoat";
      person4 = new Person(key, "eats something", 42);

      // compute a new key
      BiFunction remappingFunction = (BiFunction<String, Person, Person> & Serializable) (k, v) -> new Person(k, "eats something", 42);
      if (transactionsEnabled()) transactionManager.begin();
      cache2.compute(key, remappingFunction);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      List<Person> found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assertTrue(found.contains(person4));

      // compute and replace existing key
      BiFunction remappingExisting = (BiFunction<String, Person, Person> & Serializable) (k, v) -> new Person(k, "eats other things", 42);

      if (transactionsEnabled()) transactionManager.begin();
      cache2.compute(key, remappingExisting);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assertFalse(found.contains(person4));
      assertTrue(found.contains(new Person(key, "eats other things", 42)));

      // remove if compute result is null
      BiFunction remappingToNull = (BiFunction<String, Person, Person> & Serializable) (k, v) -> null;
      if (transactionsEnabled()) transactionManager.begin();
      cache2.compute(key, remappingToNull);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(3, found.size());
      assertFalse(found.contains(person4));
      assertFalse(found.contains(new Person(key, "eats other things", 42)));
   }

   public void testComputeIfPresent() throws Exception {
      prepareTestData();
      TransactionManager transactionManager = cache2.getAdvancedCache().getTransactionManager();
      SearchManager searchManager = Search.getSearchManager(cache2);
      QueryBuilder queryBuilder = searchManager
            .buildQueryBuilderForClass(Person.class)
            .get();
      Query allQuery = queryBuilder.all().createQuery();
      assertEquals(3, searchManager.getQuery(allQuery, Person.class).list().size());

      // compute and replace existing key
      BiFunction remappingExisting = (BiFunction<String, Person, Person> & Serializable) (k, v) -> new Person("replaced", "personOneChanged", 42);

      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfPresent(key1, remappingExisting);

      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      List<Person> found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(3, found.size());
      assertFalse(found.contains(person1));
      assertTrue(found.contains(new Person("replaced", "personOneChanged", 42)));

      String newKey = "newGoat";
      person4 = new Person(newKey, "eats something", 42);

      // do nothing for non existing keys
      BiFunction remappingFunction = (BiFunction<String, Person, Person> & Serializable) (k, v) -> new Person(k, "eats something", 42);

      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfPresent(newKey, remappingFunction);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(3, found.size());
      assertFalse(found.contains(person4));

      // remove if compute result is null
      BiFunction remappingToNull = (BiFunction<String, Person, Person> & Serializable) (k, v) -> null;
      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfPresent(key2, remappingToNull);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertFalse(found.contains(person2));
   }

   public void testComputeIfAbsent() throws Exception {
      prepareTestData();
      TransactionManager transactionManager = cache2.getAdvancedCache().getTransactionManager();
      SearchManager searchManager = Search.getSearchManager(cache2);
      QueryBuilder queryBuilder = searchManager
            .buildQueryBuilderForClass(Person.class)
            .get();
      Query allQuery = queryBuilder.all().createQuery();
      assertEquals(3, searchManager.getQuery(allQuery, Person.class).list().size());

      String key = "newGoat";
      person4 = new Person(key, "eats something", 42);

      // compute a new key
      Function mappingFunction = (Function<String, Person> & Serializable) k -> new Person(k, "eats something", 42);
      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfAbsent(key, mappingFunction);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      List<Person> found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assertTrue(found.contains(person4));

      // do nothing for existing keys
      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfAbsent(key1, mappingFunction);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assertFalse(found.contains(new Person(key1, "eats something", 42)));
      assertTrue(found.contains(person1));

      // do nothing if null
      Function mappingToNull = (Function<String, Person> & Serializable) k -> null;
      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfAbsent("anotherKey", mappingToNull);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assertFalse(found.contains(null));
   }

   public void testMerge() throws Exception {
      prepareTestData();
      TransactionManager transactionManager = cache2.getAdvancedCache().getTransactionManager();
      SearchManager searchManager = Search.getSearchManager(cache2);
      QueryBuilder queryBuilder = searchManager
            .buildQueryBuilderForClass(Person.class)
            .get();
      Query allQuery = queryBuilder.all().createQuery();
      assertEquals(3, searchManager.getQuery(allQuery, Person.class).list().size());

      String key = "newGoat";
      person4 = new Person(key, "eats something", 42);

      // merge a new key
      if (transactionsEnabled()) transactionManager.begin();
      cache2.merge(key, person4, (k1, v3) -> new Person(key, "eats something", 42));
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      List<Person> found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assertTrue(found.contains(person4));

      // merge and replace existing key
      if (transactionsEnabled()) transactionManager.begin();
      cache2.merge(key, new Person(key, "hola", 42),
            (v1, v2) -> new Person(v1.getName() + "_" + v2.getName(), v1.getBlurb() + "_" + v2.getBlurb(), v1.getAge() + v2.getAge()));
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(4, found.size());
      assertFalse(found.contains(person4));
      assertTrue(found.contains(new Person("newGoat_newGoat", "eats something_hola", 84)));

      // remove if merge result is null
      if (transactionsEnabled()) transactionManager.begin();
      cache2.merge(key, person4, (k, v) -> null);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = searchManager.<Person>getQuery(allQuery, Person.class).list();
      assertEquals(3, found.size());
      assertFalse(found.contains(person4));
      assertFalse(found.contains(new Person("newGoat_newGoat", "eats something_hola", 84)));
   }
}
