package org.infinispan.query.blackbox;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.functional.FunctionalTestUtils.await;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.IndexStatistics;
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
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class);
      return builder;
   }

   private void assertIndexesSynced() {
      List<Cache<Integer, Object>> caches = caches();
      caches.forEach(this::assertIndexesSynced);
   }

   private Map<String, Long> getIndexCountPerEntity(Cache<Integer, Object> cache) {
      IndexStatistics indexStatistics = Search.getSearchStatistics(cache).getIndexStatistics();
      Map<String, IndexInfo> stringIndexInfoMap = await(indexStatistics.computeIndexInfos().toCompletableFuture());
      return stringIndexInfoMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().count()));
   }

   private void assertIndexesSynced(Cache<Integer, Object> c) {
      String address = c.getAdvancedCache().getRpcManager().getAddress().toString();
      Map<Class<?>, AtomicInteger> countPerEntity = getEntityCountPerClass(c);
      Map<String, Long> indexInfo = getIndexCountPerEntity(c);
      countPerEntity.forEach((entity, count) -> {
         long indexed = indexInfo.get(entity.getName());
         Supplier<String> messageSupplier = () -> String.format("On node %s index contains %d entries for entity %s," +
               " but data container has %d", address, indexed, entity.getName(), count.get());
         eventually(messageSupplier, () -> indexed == count.get());
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
