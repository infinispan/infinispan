package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.distribution.Ownership.BACKUP;
import static org.infinispan.distribution.Ownership.NON_OWNER;
import static org.infinispan.distribution.Ownership.PRIMARY;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
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

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.Ownership;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

/**
 * @author Navin Surtani
 * @author Sanne Grinovero
 */
@Test(groups = {"functional"}, testName = "query.blackbox.ClusteredCacheTest")
public class ClusteredCacheTest extends MultipleCacheManagersTest {

   protected Cache<Object, Person> cache1;
   protected Cache<Object, Person> cache2;
   private Person person1;
   private Person person2;
   private Person person3;
   private Person person4;
   private Query<Person> cacheQuery;
   private final String key1 = "Navin";
   private final String key2 = "BigGoat";
   private final String key3 = "MiniGoat";

   public ClusteredCacheTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public Object[] factory() {
      return new Object[]{
            new ClusteredCacheTest().storageType(StorageType.OFF_HEAP),
            new ClusteredCacheTest().storageType(StorageType.HEAP),
      };
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      // Update the indexes as well -- see ISPN-9890
      cache(0).clear();
      super.clearContent();
   }

   protected void enhanceConfig(ConfigurationBuilder cacheCfg) {
      // meant to be overridden
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactionsEnabled());
      cacheCfg.indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class)
            .addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class);
      cacheCfg.memory()
            .storage(storageType);
      enhanceConfig(cacheCfg);
      createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);
      cache1 = cache(0);
      cache2 = cache(1);
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

   protected Query<Person> createQuery(Cache<?, ?> cache, String predicate) {
      String query = String.format("FROM %s WHERE %s", Person.class.getName(), predicate);
      return cache.query(query);
   }

   protected Query<Person> createSelectAllQuery(Cache<?, ?> cache) {
      return cache.query("FROM " + Person.class.getName());
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
      cacheQuery = createQuery(cache1, "blurb:'playing'");

      List<Person> found = cacheQuery.execute().list();

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
            extractInterceptorChain(c).getInterceptors();
   }

   public void testModified() throws Exception {
      prepareTestData();
      assertQueryInterceptorPresent(cache2);

      cacheQuery = createQuery(cache2, "blurb:'playing'");

      List<Person> found = cacheQuery.execute().list();

      assert found.size() == 1 : "Expected list of size 1, was of size " + found.size();
      assert found.get(0).equals(person1);

      person1.setBlurb("Likes pizza");
      cache1.put("Navin", person1);

      cacheQuery = createQuery(cache2, "blurb:'pizza'");

      found = cacheQuery.execute().list();

      assert found.size() == 1;
      assert found.get(0).equals(person1);
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testAdded() throws Exception {
      prepareTestData();

      cacheQuery = createQuery(cache2, "blurb:'eats'");

      List<Person> found = cacheQuery.execute().list();

      assertEquals(2, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));
      assertFalse("This should not contain object person4", found.contains(person4));

      person4 = new Person();
      person4.setName("Mighty Goat");
      person4.setBlurb("Also eats grass");

      cache1.put("mighty", person4);

      cacheQuery = createQuery(cache2, "blurb:'eats'");

      found = cacheQuery.execute().list();

      assertEquals(3, found.size());
      assertTrue(found.contains(person2));
      assertTrue(found.contains(person3));
      assertTrue("This should now contain object person4", found.contains(person4));
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testRemoved() throws Exception {
      prepareTestData();
      cacheQuery = createQuery(cache2, "blurb:'eats'");
      List<Person> found = cacheQuery.execute().list();

      assert found.size() == 2;
      assert found.contains(person2);
      assert found.contains(person3) : "This should still contain object person3";

      cache1.remove(key3);

      cacheQuery = createQuery(cache2, "blurb:'eats'");

      found = cacheQuery.execute().list();
      assert found.size() == 1;
      assert found.contains(person2);
      assert !found.contains(person3) : "This should not contain object person3 anymore";
      assert countIndex(cache1) == 2 : "Two documents should remain in the index";
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   protected int countIndex(Cache<?, ?> cache) {
      Query<?> query = cache.query("FROM " + Person.class.getName());
      return query.execute().count().value();
   }

   private Optional<Cache<Object, Person>> findCache(Ownership ownership, Object key) {
      List<Cache<Object, Person>> caches = caches();
      ClusteringDependentLogic cdl = ComponentRegistry.componentOf(cache1, ClusteringDependentLogic.class);
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

   private void testConditionalReplaceFrom(Ownership memberType) throws Exception {
      prepareTestData();

      Cache<Object, Person> cache = findCache(memberType, key1).orElse(cache2);

      assertEquals(createQuery(cache2, "blurb:'wow'").execute().list().size(), 1);

      boolean replaced = cache.replace(key1, person1, person2);

      assertTrue(replaced);
      assertEquals(createQuery(cache1, "blurb:'wow'").execute().list().size(), 0);
      assertEquals(createQuery(cache, "blurb:'wow'").execute().count().value(), 0);
   }

   private void testConditionalRemoveFrom(Ownership owneship) throws Exception {
      prepareTestData();

      Cache<Object, Person> cache = findCache(owneship, key1).orElse(cache2);

      cache.remove(key1, person1);

      assertEquals(createSelectAllQuery(cache2).execute().list().size(), 2);
      assertEquals(countIndex(cache), 2);

      cache.remove(key1, person1);

      assertEquals(createSelectAllQuery(cache2).execute().list().size(), 2);
      assertEquals(countIndex(cache), 2);

      cache = findCache(owneship, key3).orElse(cache2);

      cache.remove(key3);

      assertEquals(createSelectAllQuery(cache2).execute().list().size(), 1);
      assertEquals(countIndex(cache), 1);

      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testGetResultSize() throws Exception {
      prepareTestData();
      cacheQuery = createQuery(cache2, "blurb:'playing'");
      List<Person> found = cacheQuery.execute().list();

      assertEquals(1, found.size());
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutMap() throws Exception {
      prepareTestData();
      assertEquals(3, createSelectAllQuery(cache2).execute().list().size());

      Map<String, Person> allWrites = new HashMap<>();
      allWrites.put(key1, person1);
      allWrites.put(key2, person2);
      allWrites.put(key3, person3);

      cache2.putAll(allWrites);
      List<Person> found = createSelectAllQuery(cache2).execute().list();
      assertEquals(3, found.size());

      cache2.putAll(allWrites);
      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(3, found.size());
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutMapAsync() throws Exception {
      prepareTestData();
      assert createSelectAllQuery(cache2).execute().list().size() == 3;

      person4 = new Person();
      person4.setName("New Goat");
      person4.setBlurb("Also eats grass");

      Map<String, Person> allWrites = new HashMap<>();
      allWrites.put(key1, person1);
      allWrites.put(key2, person2);
      allWrites.put(key3, person3);
      allWrites.put("newGoat", person4);

      Future<Void> futureTask = cache2.putAllAsync(allWrites);
      futureTask.get();
      assert futureTask.isDone();
      List<Person> found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assert found.contains(person4);

      futureTask = cache1.putAllAsync(allWrites);
      futureTask.get();
      assert futureTask.isDone();
      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assert found.contains(person4);
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutForExternalRead() throws Exception {
      prepareTestData();
      assertEquals(3, createSelectAllQuery(cache2).execute().list().size());

      person4 = new Person();
      person4.setName("New Goat");
      person4.setBlurb("Also eats grass");

      cache2.putForExternalRead("newGoat", person4);
      eventually(() -> cache2.get("newGoat") != null);
      Query<Person> query = createSelectAllQuery(cache2);
      eventually(() -> {
         List<Person> found = query.execute().list();
         return found.size() == 4 && found.contains(person4);
      });

      Person person5 = new Person();
      person5.setName("Abnormal Goat");
      person5.setBlurb("Plays with grass.");
      cache2.putForExternalRead("newGoat", person5);

      eventually(() -> {
         QueryResult<Person> result = query.execute();
         List<Person> found = result.list();
         return found.size() == 4 && result.count().value() == 4 &&
               found.contains(person4) && !found.contains(person5);
      });

      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutIfAbsent() throws Exception {
      prepareTestData();
      assert createSelectAllQuery(cache2).execute().list().size() == 3;

      person4 = new Person();
      person4.setName("New Goat");
      person4.setBlurb("Also eats grass");

      cache2.putIfAbsent("newGoat", person4);

      List<Person> found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());

      assert found.contains(person4);

      Person person5 = new Person();
      person5.setName("Abnormal Goat");
      person5.setBlurb("Plays with grass.");
      cache2.putIfAbsent("newGoat", person5);

      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());

      assertFalse(found.contains(person5));
      assertTrue(found.contains(person4));
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutIfAbsentAsync() throws Exception {
      prepareTestData();
      assertEquals(3, createSelectAllQuery(cache2).execute().list().size());

      person4 = new Person();
      person4.setName("New Goat");
      person4.setBlurb("Also eats grass");

      Future<Person> futureTask = cache2.putIfAbsentAsync("newGoat", person4);
      futureTask.get();
      assertTrue(futureTask.isDone());
      List<Person> found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assertTrue(found.contains(person4));

      Person person5 = new Person();
      person5.setName("Abnormal Goat");
      person5.setBlurb("Plays with grass.");
      futureTask = cache2.putIfAbsentAsync("newGoat", person5);
      futureTask.get();
      assertTrue(futureTask.isDone());
      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assertFalse(found.contains(person5));
      assertTrue(found.contains(person4));
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testPutAsync() throws Exception {
      prepareTestData();
      assertEquals(3, createSelectAllQuery(cache2).execute().list().size());

      person4 = new Person();
      person4.setName("New Goat");
      person4.setBlurb("Also eats grass");

      Future<Person> f = cache2.putAsync("newGoat", person4);
      f.get();
      assertTrue(f.isDone());
      List<Person> found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assertTrue(found.contains(person4));

      Person person5 = new Person();
      person5.setName("Abnormal Goat");
      person5.setBlurb("Plays with grass.");
      f = cache2.putAsync("newGoat", person5);
      f.get();
      assertTrue(f.isDone());
      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assertFalse(found.contains(person4));
      assertTrue(found.contains(person5));
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testClear() throws Exception {
      prepareTestData();

      String predicate = "blurb:'eats' OR blurb:'playing'";
      Query<Person> cacheQuery1 = createQuery(cache1, predicate);
      Query<Person> cacheQuery2 = createQuery(cache2, predicate);

      assertEquals(3, cacheQuery1.execute().count().value());
      assertEquals(3, cacheQuery2.execute().count().value());

      cache2.clear();

      assertEquals(0, cacheQuery1.execute().list().size());
      assertEquals(0, cacheQuery2.execute().list().size());

      StaticTestingErrorHandler.assertAllGood(cache1, cache2);
   }

   public void testSearchKeyTransformer() throws Exception {
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

      Query<Person> cacheQuery = createQuery(cache1, "blurb:'Eats'");

      int counter = 0;
      // TODO HSEARCH-3323 Restore support for scrolling
      try (CloseableIterator<Person> found = cacheQuery.iterator()) {
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
      assertEquals(3, createSelectAllQuery(cache2).execute().list().size());

      String key = "newGoat";
      person4 = new Person(key, "eats something", 42);

      // compute a new key
      BiFunction<Object, Person, Person> remappingFunction =
            (BiFunction<Object, Person, Person> & Serializable) (k, v) -> new Person(k.toString(), "eats something", 42);
      if (transactionsEnabled()) transactionManager.begin();
      cache2.compute(key, remappingFunction);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      List<Person> found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assertTrue(found.contains(person4));

      // compute and replace existing key
      BiFunction<Object, Person, Person> remappingExisting =
            (BiFunction<Object, Person, Person> & Serializable) (k, v) -> new Person(k.toString(), "eats other things", 42);

      if (transactionsEnabled()) transactionManager.begin();
      cache2.compute(key, remappingExisting);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assertFalse(found.contains(person4));
      assertTrue(found.contains(new Person(key, "eats other things", 42)));

      // remove if compute result is null
      BiFunction<Object, Person, Person> remappingToNull = (BiFunction<Object, Person, Person> & Serializable) (k, v) -> null;
      if (transactionsEnabled()) transactionManager.begin();
      cache2.compute(key, remappingToNull);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(3, found.size());
      assertFalse(found.contains(person4));
      assertFalse(found.contains(new Person(key, "eats other things", 42)));
   }

   public void testComputeIfPresent() throws Exception {
      prepareTestData();
      TransactionManager transactionManager = cache2.getAdvancedCache().getTransactionManager();
      assertEquals(3, createSelectAllQuery(cache2).execute().list().size());

      // compute and replace existing key
      BiFunction<Object, Person, Person> remappingExisting =
            (BiFunction<Object, Person, Person> & Serializable) (k, v) -> new Person("replaced", "personOneChanged", 42);

      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfPresent(key1, remappingExisting);

      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      List<Person> found = createSelectAllQuery(cache2).execute().list();
      assertEquals(3, found.size());
      assertFalse(found.contains(person1));
      assertTrue(found.contains(new Person("replaced", "personOneChanged", 42)));

      String newKey = "newGoat";
      person4 = new Person(newKey, "eats something", 42);

      // do nothing for non existing keys
      BiFunction<Object, Person, Person> remappingFunction =
            (BiFunction<Object, Person, Person> & Serializable) (k, v) -> new Person(k.toString(), "eats something", 42);

      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfPresent(newKey, remappingFunction);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(3, found.size());
      assertFalse(found.contains(person4));

      // remove if compute result is null
      BiFunction<Object, Person, Person> remappingToNull =
            (BiFunction<Object, Person, Person> & Serializable) (k, v) -> null;
      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfPresent(key2, remappingToNull);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = createSelectAllQuery(cache2).execute().list();
      assertFalse(found.contains(person2));
   }

   public void testComputeIfAbsent() throws Exception {
      prepareTestData();
      TransactionManager transactionManager = cache2.getAdvancedCache().getTransactionManager();
      assertEquals(3, createSelectAllQuery(cache2).execute().list().size());

      String key = "newGoat";
      person4 = new Person(key, "eats something", 42);

      // compute a new key
      Function<Object, Person> mappingFunction = (Function<Object, Person> & Serializable) k -> new Person(k.toString(), "eats something", 42);
      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfAbsent(key, mappingFunction);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      List<Person> found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assertTrue(found.contains(person4));

      // do nothing for existing keys
      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfAbsent(key1, mappingFunction);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assertFalse(found.contains(new Person(key1, "eats something", 42)));
      assertTrue(found.contains(person1));

      // do nothing if null
      Function<Object, Person> mappingToNull = (Function<Object, Person> & Serializable) k -> null;
      if (transactionsEnabled()) transactionManager.begin();
      cache2.computeIfAbsent("anotherKey", mappingToNull);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assertFalse(found.contains(null));
   }

   public void testMerge() throws Exception {
      prepareTestData();
      TransactionManager transactionManager = cache2.getAdvancedCache().getTransactionManager();
      assertEquals(3, createSelectAllQuery(cache2).execute().list().size());

      String key = "newGoat";
      person4 = new Person(key, "eats something", 42);

      // merge a new key
      if (transactionsEnabled()) transactionManager.begin();
      cache2.merge(key, person4, (k1, v3) -> new Person(key, "eats something", 42));
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      List<Person> found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assertTrue(found.contains(person4));

      // merge and replace existing key
      if (transactionsEnabled()) transactionManager.begin();
      cache2.merge(key, new Person(key, "hola", 42),
            (v1, v2) -> new Person(v1.getName() + "_" + v2.getName(), v1.getBlurb() + "_" + v2.getBlurb(), v1.getAge() + v2.getAge()));
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(4, found.size());
      assertFalse(found.contains(person4));
      assertTrue(found.contains(new Person("newGoat_newGoat", "eats something_hola", 84)));

      // remove if merge result is null
      if (transactionsEnabled()) transactionManager.begin();
      cache2.merge(key, person4, (k, v) -> null);
      if (transactionsEnabled()) transactionManager.commit();
      StaticTestingErrorHandler.assertAllGood(cache1, cache2);

      found = createSelectAllQuery(cache2).execute().list();
      assertEquals(3, found.size());
      assertFalse(found.contains(person4));
      assertFalse(found.contains(new Person("newGoat_newGoat", "eats something_hola", 84)));
   }
}
