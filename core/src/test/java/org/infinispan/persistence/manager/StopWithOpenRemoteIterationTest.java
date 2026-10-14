package org.infinispan.persistence.manager;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.test.TestingUtil.extractComponent;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.persistence.support.DelayStore;
import org.infinispan.reactive.publisher.impl.PublisherHandler;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Verifies that a cache with a store can stop while it still serves an iteration for another node.
 * <p>
 * A remote iteration reads the store through a publisher that holds the persistence read lock until the stream
 * completes or is cancelled. The stream advances only when the requesting node asks for the next batch. Stopping the
 * cache on the serving node must complete even if the requester stops asking, and a requester waiting for the next
 * batch must continue the iteration from another owner.
 *
 * @since 16.3
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "persistence.manager.StopWithOpenRemoteIterationTest")
public class StopWithOpenRemoteIterationTest extends MultipleCacheManagersTest {

   private static final int NUM_KEYS = 200;
   private static final int STOP_TIMEOUT_SECONDS = 10;
   // Long enough that an iteration waiting for it can't complete within ITERATION_TIMEOUT_SECONDS.
   private static final long REMOTE_TIMEOUT_MILLIS = 20_000;
   private static final int ITERATION_TIMEOUT_SECONDS = 10;

   @Override
   protected void createCacheManagers() {
      // Each test creates the cluster it needs.
   }

   public void testStopCacheWhileServingRemoteIteration() throws Exception {
      ConfigurationBuilder builder = storeOnlyConfiguration();
      // Single owner, so node 1 is the only source for its segments.
      builder.clustering().hash().numOwners(1);
      // The server reads the store one segment at a time and releases the read lock when a segment's read completes.
      // Few segments with many entries each, so a paused iteration stays inside a segment's store read.
      builder.clustering().hash().numSegments(2);
      createClusteredCaches(2, builder);

      Cache<String, String> requester = cache(0);
      Cache<String, String> server = cache(1);
      insertKeys(requester);
      assertThat(keysServedBy(requester, address(1))).isGreaterThan(10);

      PersistenceManagerImpl serverPersistence = (PersistenceManagerImpl) extractComponent(server, PersistenceManager.class);
      // Looked up now: the registry refuses lookups once the cache is stopping.
      PublisherHandler serverHandler = extractComponent(server, PublisherHandler.class);
      // The requester reads one entry and stops asking for more, leaving the iteration open on the server.
      CacheStream<Map.Entry<String, String>> stream = requester.entrySet().stream().distributedBatchSize(1);
      Iterator<Map.Entry<String, String>> iterator = stream.iterator();
      Future<Void> stop = null;
      try {
         assertThat(iterator.hasNext()).isTrue();
         iterator.next();
         eventually(serverPersistence::anyLocksHeld);

         stop = fork(() -> server.stop());
         stop.get(STOP_TIMEOUT_SECONDS, SECONDS);
      } finally {
         if (stop != null && !stop.isDone()) {
            // Let the stuck stop finish so the test can clean up: cancel the iterations the server still serves.
            cancelServedIterations(serverHandler);
            stop.get(STOP_TIMEOUT_SECONDS, SECONDS);
         }
         stream.close();
      }
   }

   public void testIterationRetriesWhenServerStopsCache() throws Exception {
      ConfigurationBuilder builder = storeOnlyConfiguration();
      // Two owners, so every segment the server serves has another owner to read from.
      builder.clustering().hash().numOwners(2);
      builder.clustering().remoteTimeout(REMOTE_TIMEOUT_MILLIS);
      createClusteredCaches(3, builder);

      Cache<String, String> requester = cache(0);
      Cache<String, String> server = cache(1);
      insertKeys(requester);
      assertThat(keysServedBy(requester, address(1))).isGreaterThan(0);

      PublisherHandler serverHandler = extractComponent(server, PublisherHandler.class);
      DelayStore serverStore = TestingUtil.getFirstStore(server);
      // The server's store holds its first entry, so the response to the requester stays pending.
      serverStore.delayBeforeEmit(1);
      try {
         Future<Set<String>> iteration = fork(() -> {
            Set<String> keys = new HashSet<>();
            try (CacheStream<Map.Entry<String, String>> stream = requester.entrySet().stream()) {
               stream.iterator().forEachRemaining(e -> keys.add(e.getKey()));
            }
            return keys;
         });
         eventually(() -> serverHandler.openPublishers() > 0);

         server.stop();

         // The requester reads the server's segments from the other owners and completes the iteration.
         assertThat(iteration.get(ITERATION_TIMEOUT_SECONDS, SECONDS)).hasSize(NUM_KEYS);
      } finally {
         serverStore.endDelay();
      }
   }

   /**
    * A distributed cache that keeps almost every entry only in the store: the iteration reads the in-memory entries
    * first and reads the store (holding the persistence read lock) only after them. The store delays nothing unless a
    * test asks it to.
    */
   private ConfigurationBuilder storeOnlyConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.memory().maxCount(1);
      builder.persistence().addStore(DelayStore.ConfigurationBuilder.class);
      return builder;
   }

   private static void insertKeys(Cache<String, String> cache) {
      for (int i = 0; i < NUM_KEYS; i++) {
         cache.put("key-" + i, "value-" + i);
      }
   }

   /**
    * Counts the keys the requester reads from the given node: keys it doesn't own, whose primary owner is that node.
    */
   private static int keysServedBy(Cache<String, String> requester, Address server) {
      LocalizedCacheTopology topology = requester.getAdvancedCache().getDistributionManager().getCacheTopology();
      int served = 0;
      for (int i = 0; i < NUM_KEYS; i++) {
         String key = "key-" + i;
         if (!topology.isReadOwner(key) && topology.getDistribution(key).primary().equals(server))
            served++;
      }
      return served;
   }

   /**
    * Test cleanup only: closes every publisher the handler serves for other nodes.
    */
   private static void cancelServedIterations(PublisherHandler handler) {
      Map<String, ?> currentRequests = TestingUtil.extractField(handler, "currentRequests");
      List.copyOf(currentRequests.keySet()).forEach(handler::closePublisher);
   }
}
