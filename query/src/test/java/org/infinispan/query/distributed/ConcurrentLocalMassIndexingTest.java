package org.infinispan.query.distributed;

import static org.infinispan.commons.util.concurrent.CompletionStages.join;
import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStartupMode;
import org.infinispan.context.Flag;
import org.infinispan.persistence.support.DelayStore;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.impl.massindex.MassIndexerAlreadyStartedException;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Local reindexes started on different members at the same time, as happens when a cache starts with
 * {@link IndexStartupMode#REINDEX} on several members at once.
 *
 * @since 16.4
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "query.distributed.ConcurrentLocalMassIndexingTest")
public class ConcurrentLocalMassIndexingTest extends MultipleCacheManagersTest {

   private static final int NUM_NODES = 2;
   private static final int NUM_ENTITIES = 50;

   @Override
   protected void createCacheManagers() {
      // With 2 members and 2 owners, every member holds every entry, so every local reindex indexes all of them.
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cfg.persistence().addStore(DelayStore.ConfigurationBuilder.class);
      cfg.indexing().enable()
            .storage(LOCAL_HEAP)
            // The test starts every reindex itself.
            .startupMode(IndexStartupMode.NONE)
            .addIndexedEntity(Person.class);
      createClusteredCaches(NUM_NODES, QueryTestSCI.INSTANCE, cfg);
   }

   public void testLocalReindexOnAnotherMemberWhileOneIsRunning() {
      fillCache();

      Indexer indexer1 = Search.getIndexer(cache(1));
      CompletableFuture<Void> reindex0 = startHeldLocalReindex(0);
      try {
         // A local reindex on member 1 doesn't touch member 0, so it must not wait for, or fail because of, member 0.
         join(indexer1.runLocal());
         assertIndexed(1);
      } finally {
         releaseLocalReindex(0);
      }

      join(reindex0.orTimeout(30, TimeUnit.SECONDS));
      assertIndexed(0);
   }

   public void testClusterReindexFailsWhileLocalReindexIsRunning() {
      fillCache();

      Indexer indexer1 = Search.getIndexer(cache(1));
      CompletableFuture<Void> reindex0 = startHeldLocalReindex(0);
      try {
         // A cluster reindex also rebuilds member 0's index, which member 0's local reindex is rebuilding.
         CompletionException ce = assertThrows(CompletionException.class, () -> join(indexer1.run()));
         assertInstanceOf(MassIndexerAlreadyStartedException.class, Util.getRootCause(ce));
      } finally {
         releaseLocalReindex(0);
      }

      // The failed cluster reindex must not disturb the local one.
      join(reindex0.orTimeout(30, TimeUnit.SECONDS));
      assertIndexed(0);
   }

   public void testClusterReindexRunsAfterFailingOnBusyMember() {
      fillCache();

      Indexer indexer1 = Search.getIndexer(cache(1));
      CompletableFuture<Void> reindex0 = startHeldLocalReindex(0);
      try {
         assertThrows(CompletionException.class, () -> join(indexer1.run()));
      } finally {
         releaseLocalReindex(0);
      }
      join(reindex0.orTimeout(30, TimeUnit.SECONDS));

      // The failed cluster reindex must leave nothing behind that blocks the next one, on any member.
      join(indexer1.run());
      assertIndexed(0);
      assertIndexed(1);
   }

   public void testSecondLocalReindexOnSameMemberFails() {
      fillCache();

      Indexer indexer0 = Search.getIndexer(cache(0));
      CompletableFuture<Void> reindex0 = startHeldLocalReindex(0);
      try {
         // Member 0's index is already being rebuilt, so a second local reindex there must not start.
         CompletionException ce = assertThrows(CompletionException.class, () -> join(indexer0.runLocal()));
         assertInstanceOf(MassIndexerAlreadyStartedException.class, Util.getRootCause(ce));
         // Rejecting the second reindex must not end the first one.
         assertTrue(indexer0.isRunning());
      } finally {
         releaseLocalReindex(0);
      }

      // The rejected reindex must not disturb the running one.
      join(reindex0.orTimeout(30, TimeUnit.SECONDS));
      assertIndexed(0);
   }

   /**
    * Starts a local reindex on {@code member} that stays in progress until {@link #releaseLocalReindex(int)}.
    *
    * @param member the index of the member to reindex.
    * @return the stage of the local reindex.
    */
   private CompletableFuture<Void> startHeldLocalReindex(int member) {
      // Remove one entry from memory on this member only, so its local reindex must read it from the store.
      Cache<Integer, Person> cache = cache(member);
      cache.evict(0);

      // Hold the store entry back, so the local reindex stays in progress.
      DelayStore store = TestingUtil.getFirstStore(cache);
      store.delayBeforeEmit(1);

      Indexer indexer = Search.getIndexer(cache);
      CompletableFuture<Void> reindex = indexer.runLocal().toCompletableFuture();
      eventually(indexer::isRunning);
      return reindex;
   }

   /**
    * Lets the local reindex started by {@link #startHeldLocalReindex(int)} on {@code member} complete.
    *
    * @param member the index of the member whose local reindex is held.
    */
   private void releaseLocalReindex(int member) {
      DelayStore store = TestingUtil.getFirstStore(cache(member));
      store.endDelay();
   }

   private void fillCache() {
      // Skip indexing while loading, so only the reindexes under test fill the indexes.
      Map<Integer, Person> entries = new HashMap<>();
      for (int i = 0; i < NUM_ENTITIES; i++) {
         entries.put(i, new Person("name" + i, "blurb" + i, i));
      }
      this.<Integer, Person>cache(0).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).putAll(entries);
   }

   private void assertIndexed(int member) {
      Map<String, IndexInfo> indexInfos = join(Search.getSearchStatistics(cache(member))
            .getIndexStatistics().computeIndexInfos());
      assertEquals(NUM_ENTITIES, indexInfos.get(Person.class.getName()).count());
   }
}
