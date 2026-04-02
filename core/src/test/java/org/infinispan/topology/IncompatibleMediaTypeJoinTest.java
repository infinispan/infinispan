package org.infinispan.topology;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Tests that nodes with different value media type configurations can join a cluster
 * and that each node stores values in its configured media type format.
 *
 * @since 16.2
 */
@Test(groups = "functional", testName = "topology.IncompatibleMediaTypeJoinTest")
@CleanupAfterMethod
public class IncompatibleMediaTypeJoinTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "testCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      // Start with 3 nodes using APPLICATION_PROTOSTREAM encoding
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT);

      for (int i = 0; i < 3; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(TestDataSCI.INSTANCE);
         cm.defineConfiguration(CACHE_NAME, builder.build());
      }

      // Start the caches on all 3 nodes
      waitForClusterToForm(CACHE_NAME);
   }

   public void testIncompatibleValueMediaTypeJoin() throws Exception {
      // Verify the 3 existing nodes are working correctly
      Cache<String, String> cache0 = manager(0).getCache(CACHE_NAME);
      Cache<String, String> cache1 = manager(1).getCache(CACHE_NAME);
      Cache<String, String> cache2 = manager(2).getCache(CACHE_NAME);

      cache0.put("key1", "value1");
      cache0.put("key2", "value2");
      cache0.put("key3", "value3");
      cache0.put("key4", "value4");
      cache0.put("key5", "value5");
      assertEquals("value1", cache1.get("key1"));
      assertEquals("value1", cache2.get("key1"));

      log.info("Initial 3-node cluster is working correctly");

      // Now add a 4th node with a different value media type
      ConfigurationBuilder differentBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      differentBuilder.encoding().value().mediaType(MediaType.APPLICATION_PROTOSTREAM);

      EmbeddedCacheManager differentCm = addClusterEnabledCacheManager(TestDataSCI.INSTANCE);
      differentCm.defineConfiguration(CACHE_NAME, differentBuilder.build());

      log.info("Starting cache on 4th node with different media type...");

      // The 4th node should be able to join
      Cache<String, String> cache3 = differentCm.getCache(CACHE_NAME);
      waitForClusterToForm(CACHE_NAME);

      log.info("4th node joined successfully");

      // Put data from the new node
      cache3.put("key6", "value6");
      cache3.put("key7", "value7");

      // Verify the new node can read its own data
      assertEquals("value6", cache3.get("key6"));
      assertEquals("value7", cache3.get("key7"));

      // Now verify that each node stores values in its configured media type format
      // Check the first 3 nodes (APPLICATION_OBJECT)
      verifyStoredMediaType(cache0, "key1", MediaType.APPLICATION_OBJECT);
      verifyStoredMediaType(cache1, "key1", MediaType.APPLICATION_OBJECT);
      verifyStoredMediaType(cache2, "key1", MediaType.APPLICATION_OBJECT);
      verifyStoredMediaType(cache3, "key1", MediaType.APPLICATION_PROTOSTREAM);

      // The 4th node should store values in PROTOSTREAM format
      verifyStoredMediaType(cache3, "key6", MediaType.APPLICATION_PROTOSTREAM);
      verifyStoredMediaType(cache3, "key7", MediaType.APPLICATION_PROTOSTREAM);
   }

   private void verifyStoredMediaType(Cache<String, String> cache, String key, MediaType expectedMediaType) {
      AdvancedCache<String, String> advancedCache = cache.getAdvancedCache();
      InternalDataContainer<String, String> dataContainer = TestingUtil.extractComponent(advancedCache, InternalDataContainer.class);
      InternalCacheEntry<String, String> entry = dataContainer.peek(key);

      if (entry != null) {
         // Entry is stored on this node, verify its actual stored value type
         Object storedValue = entry.getValue();
         Class<?> storedClass = storedValue.getClass();

         if (expectedMediaType.equals(MediaType.APPLICATION_OBJECT)) {
            // APPLICATION_OBJECT should store values as their native Java type (String in this case)
            assertEquals("Node " + advancedCache.getRpcManager().getAddress() +
                  " should store values as String for APPLICATION_OBJECT, but got " + storedClass.getName(),
                  String.class, storedClass);
            log.infof("Verified node %s stores key %s as String (APPLICATION_OBJECT)",
                  advancedCache.getRpcManager().getAddress(), key);
         } else if (expectedMediaType.equals(MediaType.APPLICATION_PROTOSTREAM)) {
            // APPLICATION_PROTOSTREAM should store values as byte arrays
            assertEquals("Node " + advancedCache.getRpcManager().getAddress() +
                  " should store values as byte[] for APPLICATION_PROTOSTREAM, but got " + storedClass.getName(),
                  byte[].class, storedClass);
            log.infof("Verified node %s stores key %s as byte[] (APPLICATION_PROTOSTREAM)",
                  advancedCache.getRpcManager().getAddress(), key);
         } else {
            fail("Unknown media type: " + expectedMediaType);
         }
      } else {
         log.infof("Key %s not stored on node %s (not an owner)", key, advancedCache.getRpcManager().getAddress());
      }
   }
}
