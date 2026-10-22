package org.infinispan.server.hotrod.event;

import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.server.hotrod.HotRodMultiNodeTest;
import org.infinispan.server.hotrod.test.TestResponse;
import org.testng.annotations.Test;

/**
 * Primary near cache listeners are registered on the nodes of a cluster, which publish the owners of the segments
 * through the topology cache, so they are tested with a clustered server.
 */
@Test(groups = "functional", testName = "server.hotrod.event.HotRodPrimaryNearCacheListenerTest")
public class HotRodPrimaryNearCacheListenerTest extends HotRodMultiNodeTest {

   @Override
   protected String cacheName() {
      return "primaryNearCacheListenerCache";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }

   public void testPrimaryNearCacheListenerProtocol() {
      Cache<?, ?> cache = cache(0, cacheName());
      EventLogListener eventListener = new EventLogListener();
      byte[] key = primaryKey(cache, "k-");
      clients().get(0).put(key, 0, 0, value("v1"));

      TestResponse response = clients().get(0).addPrimaryNearCacheListener(eventListener, 0);
      try {
         assertStatus(response, Success);
         clients().get(0).put(key, 0, 0, value("v2"));
         eventListener.expectOnlyModifiedEvent(cache, key);
         clients().get(0).remove(key);
         eventListener.expectOnlyRemovedEvent(cache, key);
      } finally {
         assertStatus(clients().get(0).removeClientListener(eventListener.getId()), Success);
      }
   }

   /**
    * A primary listener can be removed and re-added on the same connection (e.g. when a server temporarily stops
    * being a primary owner for any segment). The bloom filter used by the new listener must be the one that
    * reads add keys to, otherwise invalidations are silently dropped.
    */
   public void testPrimaryNearCacheListenerWithBloomFilterReAdded() {
      Cache<?, ?> cache = cache(0, cacheName());
      EventLogListener eventListener = new EventLogListener();
      byte[] readKey = primaryKey(cache, "read-");
      byte[] unreadKey = primaryKey(cache, "unread-");
      clients().get(0).put(readKey, 0, 0, value("v1"));
      clients().get(0).put(unreadKey, 0, 0, value("v1"));

      int bloomBits = 1024;
      assertStatus(clients().get(0).addPrimaryNearCacheListener(eventListener, bloomBits), Success);
      assertStatus(clients().get(0).removeClientListener(eventListener.getId()), Success);

      assertStatus(clients().get(0).addPrimaryNearCacheListener(eventListener, bloomBits), Success);
      try {
         // Reading adds the key to the bloom filter of the current listener
         assertStatus(clients().get(0).getWithMetadata(readKey, 0), Success);

         // Key was never read so the bloom filter must filter out the event
         clients().get(0).put(unreadKey, 0, 0, value("v2"));
         eventListener.expectNoEvents(Optional.empty());

         clients().get(0).put(readKey, 0, 0, value("v2"));
         eventListener.expectOnlyModifiedEvent(cache, readKey);
      } finally {
         assertStatus(clients().get(0).removeClientListener(eventListener.getId()), Success);
      }
   }

   public void testRemovingOldPrimaryListenerPreservesReplacementBloomFilter() {
      assertReplacementBloomFilterSurvivesRemoval(true);
   }

   public void testRemovingOldPrimaryListenerPreservesClusterBloomFilter() {
      assertReplacementBloomFilterSurvivesRemoval(false);
   }

   private void assertReplacementBloomFilterSurvivesRemoval(boolean primaryReplacement) {
      Cache<?, ?> cache = cache(0, cacheName());
      EventLogListener oldListener = new EventLogListener();
      EventLogListener replacement = new EventLogListener() {
         @Override
         public byte[] getId() {
            return new byte[]{4, 5, 6};
         }
      };
      byte[] readKey = primaryKey(cache, "replacement-read-");
      byte[] unreadKey = primaryKey(cache, "replacement-unread-");
      var client = clients().get(0);
      client.put(readKey, 0, 0, value("v1"));
      client.put(unreadKey, 0, 0, value("v1"));

      assertStatus(client.addPrimaryNearCacheListener(oldListener, 1024), Success);
      try {
         TestResponse response = primaryReplacement
               ? client.addPrimaryNearCacheListener(replacement, 1024)
               : client.addBloomNearCacheListener(replacement, 1024);
         assertStatus(response, Success);
         // Cleanup from the old registration can arrive after its replacement has installed a filter.
         assertStatus(client.removeClientListener(oldListener.getId()), Success);
         assertStatus(client.getWithMetadata(readKey, 0), Success);

         client.put(unreadKey, 0, 0, value("v2"));
         replacement.expectNoEvents(Optional.empty());
         client.put(readKey, 0, 0, value("v2"));
         replacement.expectOnlyModifiedEvent(cache, readKey);
      } finally {
         client.removeClientListener(oldListener.getId());
         client.removeClientListener(replacement.getId());
      }
   }

   /** Returns a key whose primary owner is the node of the cache. */
   private static byte[] primaryKey(Cache<?, ?> cache, String prefix) {
      LocalizedCacheTopology topology = cache.getAdvancedCache().getDistributionManager().getCacheTopology();
      return IntStream.range(0, Integer.MAX_VALUE)
            .mapToObj(i -> (prefix + i).getBytes(StandardCharsets.UTF_8))
            .filter(key -> topology.getDistribution(key).isPrimary())
            .findFirst()
            .orElseThrow();
   }

   private static byte[] value(String value) {
      return value.getBytes(StandardCharsets.UTF_8);
   }
}
