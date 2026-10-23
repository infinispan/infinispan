package org.infinispan.query.distributed;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.support.FailStore;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestException;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(groups = "functional", testName = "query.distributed.MassIndexerStoreFailureTest")
public class MassIndexerStoreFailureTest extends SingleCacheManagerTest {

   // The in-memory entries are published before the store entries, so the store failure, or stall, only arrives
   // after all of them were iterated. There must be more entries than the reindex consumes when it starts, so the
   // failure arrives later, while index updates complete.
   // The implementation consumes up to 100 * 128 entries at start.
   private static final int NUM_ENTITIES = 20_000;

   private static final int PUT_BATCH_SIZE = 1_000;

   // Entries removed from memory, so they are only published by the store.
   private static final int STORE_ONLY_ENTITIES = 10;

   // The store publisher fails when it takes longer than this between two entries.
   private static final long STORE_EMIT_TIMEOUT_MS = 1_000;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(false);
      cfg.clustering().remoteTimeout(STORE_EMIT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      cfg.persistence().addStore(FailStore.ConfigurationBuilder.class);
      cfg.indexing().enable()
            .storage(LOCAL_HEAP)
            // Persistent data with volatile indexes would reindex at startup.
            // We want for the test to trigger re-index so we can assert everything.
            .startupMode(IndexStartupMode.NONE)
            .addIndexedEntity(Person.class);
      return TestCacheManagerFactory.createCacheManager(QueryTestSCI.INSTANCE, cfg);
   }

   public void testReindexFailsWhenStoreFailsMidIteration() {
      fillCache();

      // The first store publisher fails when subscribed. The in-memory entries are published before the store,
      // so the reindex fails after it has already processed entries.
      FailStore store = TestingUtil.getFirstStore(cache);
      store.failPublish(1);

      assertReindexFails(TestException.class);
   }

   public void testReindexFailsWhenStoreExceedsTimeBetweenEntries() {
      fillCache();

      for (int i = 0; i < STORE_ONLY_ENTITIES; i++) {
         cache.evict(i);
      }

      // Hold the first store-only entry back until the store publisher times out. The reindex waits for the store
      // after it has already processed the in-memory entries.
      FailStore store = TestingUtil.getFirstStore(cache);
      store.delayBeforeEmit(1);
      try {
         assertReindexFails(TimeoutException.class);
      } finally {
         store.endDelay();
      }
   }

   private void fillCache() {
      // The reindex rebuilds the index, so skip indexing while loading to keep the setup fast.
      Map<Integer, Person> batch = new HashMap<>();
      for (int i = 0; i < NUM_ENTITIES; i++) {
         batch.put(i, new Person("name" + i, "blurb" + i, i));
         if (batch.size() == PUT_BATCH_SIZE) {
            cache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING).putAll(batch);
            batch.clear();
         }
      }
   }

   private void assertReindexFails(Class<? extends Throwable> expectedCause) {
      Indexer indexer = Search.getIndexer(cache);
      CompletableFuture<Void> reindex = indexer.run().toCompletableFuture();

      ExecutionException ee = assertThrows(ExecutionException.class,
            () -> reindex.get(30, TimeUnit.SECONDS));
      assertInstanceOf(expectedCause, Util.getRootCause(ee));
      assertFalse(indexer.isRunning());
   }
}
