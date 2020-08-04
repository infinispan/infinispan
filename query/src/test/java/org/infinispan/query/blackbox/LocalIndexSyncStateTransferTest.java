package org.infinispan.query.blackbox;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.lucene.index.DirectoryReader;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.helper.IndexAccessor;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Tests for correctness of local indexes syncing during state transfer
 */
@Test(groups = "functional", testName = "query.blackbox.LocalIndexStateTransferTest")
public class LocalIndexSyncStateTransferTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(2, QueryTestSCI.INSTANCE, getBuilder());
   }

   protected ConfigurationBuilder getBuilder() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.indexing().enable()
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP);
      return builder;
   }

   private void assertIndexesSynced() {
      List<Cache<Integer, Object>> caches = caches();
      caches.forEach(this::assertIndexesSynced);
   }

   private void assertIndexesSynced(Cache<Integer, Object> c) {
      String address = c.getAdvancedCache().getRpcManager().getAddress().toString();
      Map<Class<?>, AtomicInteger> countPerEntity = getEntityCountPerClass(c);
      countPerEntity.forEach((entity, count) -> {
         DirectoryReader reader = IndexAccessor.of(c, entity).getIndexReader();
         Supplier<String> messageSupplier = () -> String.format("On node %s index contains %d entries for entity %s," +
               " but data container has %d", address, reader.numDocs(), entity.getName(), count.get());
         eventually(messageSupplier, () -> reader.numDocs() == count.get());
      });
   }

   protected Map<Class<?>, AtomicInteger> getEntityCountPerClass(Cache<Integer, Object> c) {
      Map<Class<?>, AtomicInteger> countPerEntity = new HashMap<>();
      c.getAdvancedCache().getDataContainer().forEach(e -> {
         Class<?> entity = e.getValue().getClass();
         countPerEntity.computeIfAbsent(entity, aClass -> new AtomicInteger(0)).incrementAndGet();
      });
      return countPerEntity;
   }

   public void testIndexSyncedDuringST() {
      // Populate caches
      Cache<Integer, Person> cache = cache(0);
      for (int i = 0; i < 10; i++) {
         cache.put(i, new Person("person" + i, "blurb" + i, i + 10));
      }
      assertIndexesSynced();

      // Add a new Node
      addClusterEnabledCacheManager(QueryTestSCI.INSTANCE, getBuilder()).getCache();
      assertIndexesSynced();

      // Remove a node
      killMember(2);
      assertIndexesSynced();
   }
}
