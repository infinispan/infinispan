package org.infinispan.server.functional.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.multimap.MetadataCollection;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCache;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.server.functional.ClusteredIT;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HotRodMultiMapOperations {

   @RegisterExtension
   public static InfinispanServerExtension SERVERS = ClusteredIT.SERVERS;

   @Test
   public void testMultiMap() {
      RemoteMultimapCache<Integer, String> people = multimapCache();
      CompletableFuture<Void> elaia = people.put(1, "Elaia");
      people.put(1, "Oihana").join();
      elaia.join();

      Collection<String> littles = people.get(1).join();

      assertEquals(2, littles.size());
      assertTrue(littles.contains("Elaia"));
      assertTrue(littles.contains("Oihana"));
   }

   @Test
   public void testPutWithDuplicates() {
      RemoteMultimapCache<String, String> multimapCache = multimapCache();

      multimapCache.put("k", "a").join();
      multimapCache.put("k", "a").join();
      multimapCache.put("k", "a").join();

      Collection<String> kValues = multimapCache.get("k").join();

      assertEquals(1, kValues.size());
      assertTrue(kValues.contains("a"));
   }

   @Test
   public void testGetWithMetadata() {
      RemoteMultimapCache<String, String> multimapCache = multimapCache();
      multimapCache.put("k", "a").join();

      MetadataCollection<String> metadataCollection = multimapCache.getWithMetadata("k").join();

      assertEquals(1, metadataCollection.getCollection().size());
      assertTrue(metadataCollection.getCollection().contains("a"));
      assertEquals(-1, metadataCollection.getLifespan());
      assertEquals(0, metadataCollection.getVersion());
   }

   @Test
   public void testRemoveKey() {
      RemoteMultimapCache<String, String> multimapCache = multimapCache();
      multimapCache.put("k", "a").join();
      Collection<String> kValues = multimapCache.get("k").join();
      assertEquals(1, kValues.size());
      assertTrue(kValues.contains("a"));

      assertTrue(multimapCache.remove("k").join());
      assertEquals(0, multimapCache.get("k").join().size());
      assertFalse(multimapCache.remove("k").join());
   }

   @Test
   public void testRemoveKeyValue() {
      RemoteMultimapCache<String, String> multimapCache = multimapCache();
      multimapCache.put("k", "a").join();
      multimapCache.put("k", "b").join();
      multimapCache.put("k", "c").join();
      Collection<String> kValues = multimapCache.get("k").join();
      assertEquals(3, kValues.size());

      assertTrue(multimapCache.remove("k", "a").join());
      assertEquals(2, multimapCache.get("k").join().size());
   }

   @Test
   public void testSize() {
      RemoteMultimapCache<String, String> multimapCache = multimapCache();
      assertEquals(Long.valueOf(0), multimapCache.size().join());
      multimapCache.put("k", "a").join();
      assertEquals(Long.valueOf(1), multimapCache.size().join());
      multimapCache.put("k", "b").join();
      assertEquals(Long.valueOf(2), multimapCache.size().join());
   }

   @Test
   public void testContainsEntry() {
      RemoteMultimapCache<String, String> multimapCache = multimapCache();
      multimapCache.put("k", "a").join();
      assertTrue(multimapCache.containsEntry("k", "a").join());
      assertFalse(multimapCache.containsEntry("k", "b").join());
   }

   @Test
   public void testContainsKey() {
      RemoteMultimapCache<String, String> multimapCache = multimapCache();
      multimapCache.put("k", "a").join();
      assertTrue(multimapCache.containsKey("k").join());
      assertFalse(multimapCache.containsKey("l").join());
   }

   @Test
   public void testContainsValue() {
      RemoteMultimapCache<String, String> multimapCache = multimapCache();
      multimapCache.put("k", "a").join();
      assertTrue(multimapCache.containsValue("a").join());
      assertFalse(multimapCache.containsValue("b").join());
   }

   private <K, V> RemoteMultimapCache<K, V> multimapCache() {
      RemoteCache<K, V> cache = SERVERS.hotrod().create();
      MultimapCacheManager<K, V> multimapCacheManager = RemoteMultimapCacheManagerFactory.from(cache.getRemoteCacheManager());

      return multimapCacheManager.get(cache.getName());
   }
}
