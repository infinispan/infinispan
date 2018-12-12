package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Consumer;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.blackbox.IndexingWithPersistenceTest")
public class IndexingWithPersistenceTest extends SingleCacheManagerTest {
   private static final String KEY = "k";
   private static final Person RADIM = new Person("Radim", "Tough guy!", 29);
   private static final Person DAN = new Person("Dan", "Not that tough.", 39);
   private static final AnotherGrassEater FLUFFY = new AnotherGrassEater("Fluffy", "Very cute.");
   private DummyInMemoryStore store;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.indexing()
            .index(Index.ALL)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      builder.persistence().addStore(new DummyInMemoryStoreConfigurationBuilder(builder.persistence()));
      EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(builder);
      cache = cacheManager.getCache();
      store = cache.getAdvancedCache().getComponentRegistry().getComponent(PersistenceManager.class)
            .getStores(DummyInMemoryStore.class).iterator().next();
      return cacheManager;
   }

   public void testPut() {
      test(c -> c.put(KEY, FLUFFY), this::assertFluffyIndexed, false);
   }

   public void testPutIgnoreReturnValue() {
      test(c -> c.getAdvancedCache().withFlags(Flag.IGNORE_RETURN_VALUES).put(KEY, FLUFFY), this::assertFluffyIndexed, false);
   }

   public void testPutMap() {
      test(c -> c.putAll(Collections.singletonMap(KEY, FLUFFY)), this::assertFluffyIndexed, false);
   }

   public void testReplace() {
      test(c -> c.replace(KEY, FLUFFY), this::assertFluffyIndexed, false);
   }

   public void testRemove() {
      test(c -> c.remove(KEY), sm -> {}, true);
   }

   public void testCompute() {
      test(c -> c.compute(KEY, (k, old) -> FLUFFY), this::assertFluffyIndexed, false);
   }

   public void testComputeRemove() {
      test(c -> c.compute(KEY, (k, old) -> null), sm -> {}, true);
   }

   public void testMerge() {
      test(c -> c.merge(KEY, FLUFFY, (o, n) -> n), this::assertFluffyIndexed, false);
   }

   public void testMergeRemove() {
      test(c -> c.merge(KEY, FLUFFY, (o, n) -> null), sm -> {}, true);
   }

   private void test(Consumer<Cache> op, Consumer<SearchManager> check, boolean remove) {
      // insert entity and make it evicted (move to the store)
      cache.put(KEY, RADIM);

      // to prevent the case when index becomes empty (and we mistake it for correctly removed item) we add another person
      cache.put("k2", DAN);

      // it should be indexed
      SearchManager sm = Search.getSearchManager(cache);
      List<Person> found = queryAll(sm, Person.class);
      assertEquals(Arrays.asList(RADIM, DAN), sortByAge(found));

      // evict
      cache.evict(KEY);

      // check the entry is in the store
      MarshallableEntry inStore = store.loadEntry(KEY);
      assertNotNull(inStore);
      assertTrue("In store: " + inStore, inStore.getValue() instanceof Person);
      // ...and not in the container
      assertEquals(null, cache.getAdvancedCache().getDataContainer().get(KEY));

      // do the modification
      op.accept(cache);

      // now the person should be gone
      assertEquals(Collections.singletonList(DAN), queryAll(sm, Person.class));
      // and the indexes for other type should be updated
      check.accept(sm);

      InternalCacheEntry ice = cache.getAdvancedCache().getDataContainer().get(KEY);
      inStore = store.loadEntry(KEY);
      if (remove) {
         assertEquals(null, ice);
         assertEquals(null, inStore);
      } else {
         assertNotNull(ice);
         assertTrue("In DC: " + ice, ice.getValue() instanceof AnotherGrassEater);
         assertNotNull(inStore);
         assertTrue("In store: " + inStore, inStore.getValue() instanceof AnotherGrassEater);
      }
   }

   private List<Person> sortByAge(List<Person> people) {
      Collections.sort(people, Comparator.comparingInt(Person::getAge));
      return people;
   }

   private <T> List<T> queryAll(SearchManager sm, Class<T> entityType) {
      return sm.<T>getQuery(sm.buildQueryBuilderForClass(entityType).get().all().createQuery(), entityType).list();
   }

   private void assertFluffyIndexed(SearchManager sm) {
      assertEquals(Collections.singletonList(FLUFFY), queryAll(sm, AnotherGrassEater.class));
   }
}
