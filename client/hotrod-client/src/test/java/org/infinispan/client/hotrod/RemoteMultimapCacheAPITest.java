package org.infinispan.client.hotrod;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.multimap.MetadataCollection;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCache;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.testng.annotations.Test;

/**
 * Multimap Cache Remote test
 *
 * @author karesti@redhat.com
 * @since 9.2
 */
@Test(groups = "functional", testName = "client.hotrod.RemoteMultimapCacheAPITest")
public class RemoteMultimapCacheAPITest extends SingleHotRodServerTest {

   private RemoteMultimapCache<String, String> mutimapCache;

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.forceReturnValues(isForceReturnValuesViaConfiguration());
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      return new InternalRemoteCacheManager(builder.build());
   }

   @Override
   protected void setup() throws Exception {
      cacheManager = createCacheManager();
      hotrodServer = createHotRodServer();
      remoteCacheManager = getRemoteCacheManager();
      remoteCacheManager.start();
      MultimapCacheManager rmc = RemoteMultimapCacheManagerFactory.from(remoteCacheManager);
      // TODO: Avoid conflit names with a namespace or configuration between cache and multimap cache
      this.mutimapCache = rmc.get("");
   }

   protected boolean isForceReturnValuesViaConfiguration() {
      return true;
   }

   public void testGetNotExist() throws Exception {
      Collection<String> kValues = mutimapCache.get("k").join();
      assertEquals(0, kValues.size());
   }

   public void testGetWithMetadataNotExist() throws Exception {
      MetadataCollection<String> k = mutimapCache.getWithMetadata("k").join();
      assertEquals(0, k.getCollection().size());
   }

   public void testPut() throws Exception {
      mutimapCache.put("k", "a").join();
      mutimapCache.put("k", "b").join();
      mutimapCache.put("k", "c").join();

      Collection<String> kValues = mutimapCache.get("k").join();

      assertEquals(3, kValues.size());
      assertTrue(kValues.contains("a"));
      assertTrue(kValues.contains("b"));
      assertTrue(kValues.contains("c"));
   }

   public void testPutWithDuplicates() throws Exception {
      mutimapCache.put("k", "a").join();
      mutimapCache.put("k", "a").join();
      mutimapCache.put("k", "a").join();

      Collection<String> kValues = mutimapCache.get("k").join();

      assertEquals(1, kValues.size());
      assertTrue(kValues.contains("a"));
   }

   public void testGetWithMetadata() throws Exception {
      mutimapCache.put("k", "a").join();

      MetadataCollection<String> metadataCollection = mutimapCache.getWithMetadata("k").join();

      assertEquals(1, metadataCollection.getCollection().size());
      assertTrue(metadataCollection.getCollection().contains("a"));
      assertEquals(-1, metadataCollection.getLifespan());
      assertEquals(0, metadataCollection.getVersion());
   }

   public void testRemoveKey() throws Exception {
      mutimapCache.put("k", "a").join();
      Collection<String> kValues = mutimapCache.get("k").join();
      assertEquals(1, kValues.size());
      assertTrue(kValues.contains("a"));

      assertTrue(mutimapCache.remove("k").join());
      assertEquals(0, mutimapCache.get("k").join().size());
      assertFalse(mutimapCache.remove("k").join());
   }

   public void testRemoveKeyValue() throws Exception {
      mutimapCache.put("k", "a").join();
      mutimapCache.put("k", "b").join();
      mutimapCache.put("k", "c").join();
      Collection<String> kValues = mutimapCache.get("k").join();
      assertEquals(3, kValues.size());

      assertTrue(mutimapCache.remove("k", "a").join());
      assertEquals(2, mutimapCache.get("k").join().size());
   }

   public void testSize() throws Exception {
      assertEquals(Long.valueOf(0), mutimapCache.size().join());
      mutimapCache.put("k", "a").join();
      assertEquals(Long.valueOf(1), mutimapCache.size().join());
      mutimapCache.put("k", "b").join();
      assertEquals(Long.valueOf(2), mutimapCache.size().join());
   }

   public void testContainsEntry() throws Exception {
      mutimapCache.put("k", "a").join();
      assertTrue(mutimapCache.containsEntry("k", "a").join());
      assertFalse(mutimapCache.containsEntry("k", "b").join());
   }

   public void testContainsKey() throws Exception {
      mutimapCache.put("k", "a").join();
      assertTrue(mutimapCache.containsKey("k").join());
      assertFalse(mutimapCache.containsKey("l").join());
   }

   public void testContainsValue() throws Exception {
      mutimapCache.put("k", "a").join();
      assertTrue(mutimapCache.containsValue("a").join());
      assertFalse(mutimapCache.containsValue("b").join());
   }
}
