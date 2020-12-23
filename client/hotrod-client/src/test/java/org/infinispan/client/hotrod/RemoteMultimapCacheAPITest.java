package org.infinispan.client.hotrod;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.multimap.MetadataCollection;
import org.infinispan.client.hotrod.multimap.MultimapCacheManager;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCache;
import org.infinispan.client.hotrod.multimap.RemoteMultimapCacheManagerFactory;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
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

   private RemoteMultimapCache<String, String> multimapCache;

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      // Do not retry when the server response cannot be parsed, see ISPN-12596
      builder.forceReturnValues(isForceReturnValuesViaConfiguration()).maxRetries(0);
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort());
      return new InternalRemoteCacheManager(builder.build());
   }

   @Override
   protected void setup() throws Exception {
      cacheManager = createCacheManager();
      hotrodServer = createHotRodServer();
      remoteCacheManager = getRemoteCacheManager();
      remoteCacheManager.start();
      MultimapCacheManager<String, String> rmc = RemoteMultimapCacheManagerFactory.from(remoteCacheManager);
      // TODO: Avoid conflict names with a namespace or configuration between cache and multimap cache
      this.multimapCache = rmc.get("");
   }

   protected boolean isForceReturnValuesViaConfiguration() {
      return true;
   }

   public void testGetNotExist() throws Exception {
      Collection<String> kValues = multimapCache.get("k").join();
      assertEquals(0, kValues.size());
   }

   public void testGetWithMetadataNotExist() throws Exception {
      CompletableFuture<MetadataCollection<String>> k = multimapCache.getWithMetadata("k");
      assertEquals(0, k.join().getCollection().size());
   }

   public void testPut() throws Exception {
      multimapCache.put("k", "a").join();
      multimapCache.put("k", "b").join();
      multimapCache.put("k", "c").join();

      Collection<String> kValues = multimapCache.get("k").join();

      assertEquals(3, kValues.size());
      assertTrue(kValues.contains("a"));
      assertTrue(kValues.contains("b"));
      assertTrue(kValues.contains("c"));
   }

   public void testPutWithDuplicates() throws Exception {
      multimapCache.put("k", "a").join();
      multimapCache.put("k", "a").join();
      multimapCache.put("k", "a").join();

      Collection<String> kValues = multimapCache.get("k").join();

      assertEquals(1, kValues.size());
      assertTrue(kValues.contains("a"));
   }

   public void testGetWithMetadata() throws Exception {
      multimapCache.put("k", "a").join();

      MetadataCollection<String> metadataCollection = multimapCache.getWithMetadata("k").join();

      assertEquals(1, metadataCollection.getCollection().size());
      assertTrue(metadataCollection.getCollection().contains("a"));
      assertEquals(-1, metadataCollection.getLifespan());
      assertEquals(0, metadataCollection.getVersion());
   }

   public void testRemoveKey() throws Exception {
      multimapCache.put("k", "a").join();
      Collection<String> kValues = multimapCache.get("k").join();
      assertEquals(1, kValues.size());
      assertTrue(kValues.contains("a"));

      assertTrue(multimapCache.remove("k").join());
      assertEquals(0, multimapCache.get("k").join().size());
      assertFalse(multimapCache.remove("k").join());
   }

   public void testRemoveKeyValue() throws Exception {
      multimapCache.put("k", "a").join();
      multimapCache.put("k", "b").join();
      multimapCache.put("k", "c").join();
      Collection<String> kValues = multimapCache.get("k").join();
      assertEquals(3, kValues.size());

      assertTrue(multimapCache.remove("k", "a").join());
      assertEquals(2, multimapCache.get("k").join().size());
   }

   public void testSize() throws Exception {
      assertEquals(Long.valueOf(0), multimapCache.size().join());
      multimapCache.put("k", "a").join();
      assertEquals(Long.valueOf(1), multimapCache.size().join());
      multimapCache.put("k", "b").join();
      assertEquals(Long.valueOf(2), multimapCache.size().join());
   }

   public void testContainsEntry() throws Exception {
      multimapCache.put("k", "a").join();
      assertTrue(multimapCache.containsEntry("k", "a").join());
      assertFalse(multimapCache.containsEntry("k", "b").join());
   }

   public void testContainsKey() throws Exception {
      multimapCache.put("k", "a").join();
      assertTrue(multimapCache.containsKey("k").join());
      assertFalse(multimapCache.containsKey("l").join());
   }

   public void testContainsValue() throws Exception {
      multimapCache.put("k", "a").join();
      assertTrue(multimapCache.containsValue("a").join());
      assertFalse(multimapCache.containsValue("b").join());
   }
}
