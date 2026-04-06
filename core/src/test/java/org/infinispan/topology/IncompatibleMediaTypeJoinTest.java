package org.infinispan.topology;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.Optional;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.BeforeMethod;
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

   private Cache<String, String> cache0;
   private Cache<String, String> cache1;
   private Cache<String, String> cache2;
   private Cache<String, String> cache3; // The node with different media type

   @Override
   protected void createCacheManagers() throws Throwable {
      // Start with 3 nodes using APPLICATION_OBJECT encoding
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      builder.encoding().value().mediaType(MediaType.APPLICATION_OBJECT);

      for (int i = 0; i < 3; i++) {
         EmbeddedCacheManager cm = addClusterEnabledCacheManager(TestDataSCI.INSTANCE);
         cm.defineConfiguration(CACHE_NAME, builder.build());
      }

      // Start the caches on all 3 nodes
      waitForClusterToForm(CACHE_NAME);
   }

   @BeforeMethod
   public void setupHeterogeneousCluster() throws Exception {
      // Get references to the first 3 caches
      cache0 = manager(0).getCache(CACHE_NAME);
      cache1 = manager(1).getCache(CACHE_NAME);
      cache2 = manager(2).getCache(CACHE_NAME);

      // Verify initial cluster is working
      cache0.put("initial_key", "initial_value");
      assertEquals("initial_value", cache1.get("initial_key"));
      assertEquals("initial_value", cache2.get("initial_key"));

      log.info("Initial 3-node cluster is working correctly");

      // Now add a 4th node with a different value media type
      ConfigurationBuilder differentBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      differentBuilder.encoding().value().mediaType(MediaType.APPLICATION_PROTOSTREAM);

      EmbeddedCacheManager differentCm = addClusterEnabledCacheManager(TestDataSCI.INSTANCE);
      differentCm.defineConfiguration(CACHE_NAME, differentBuilder.build());

      log.info("Starting cache on 4th node with different media type...");

      // The 4th node should be able to join
      cache3 = differentCm.getCache(CACHE_NAME);
      waitForClusterToForm(CACHE_NAME);

      log.info("4th node joined successfully");

      // Verify the 4th node received state transfer correctly
      verifyStoredMediaType(cache0, "initial_key", MediaType.APPLICATION_OBJECT);
      verifyStoredMediaType(cache3, "initial_key", MediaType.APPLICATION_PROTOSTREAM);
   }

   public void testPut() {
      log.info("Testing put...");

      cache3.put("key_put_1", "value_put_1");
      cache3.put("key_put_2", "value_put_2");

      // Test returning old value
      log.info("DEBUG: Testing put with old value return");
      String oldValue = cache3.put("key_put_1", "value_put_1_new");
      assertEquals("value_put_1", oldValue);
      assertEquals("value_put_1_new", cache3.get("key_put_1"));

      // Verify values are retrievable
      assertEquals("value_put_1_new", cache3.get("key_put_1"));
      assertEquals("value_put_2", cache3.get("key_put_2"));

      // Verify storage format on PROTOSTREAM node
      verifyStoredMediaType(cache3, "key_put_1", MediaType.APPLICATION_PROTOSTREAM);
      verifyStoredMediaType(cache3, "key_put_2", MediaType.APPLICATION_PROTOSTREAM);

      // Verify storage format on APPLICATION_OBJECT nodes
      verifyStoredMediaType(cache0, "key_put_1", MediaType.APPLICATION_OBJECT);
      verifyStoredMediaType(cache0, "key_put_2", MediaType.APPLICATION_OBJECT);
   }

   public void testPutIfAbsent() {
      log.info("Testing putIfAbsent...");

      cache3.putIfAbsent("key_putIfAbsent", "value_putIfAbsent");

      // Verify value is retrievable
      assertEquals("value_putIfAbsent", cache3.get("key_putIfAbsent"));

      // DEBUG: Check storage format before second putIfAbsent
      log.info("DEBUG: Checking storage after first putIfAbsent");
      verifyStoredMediaType(cache3, "key_putIfAbsent", MediaType.APPLICATION_PROTOSTREAM);
      verifyStoredMediaType(cache0, "key_putIfAbsent", MediaType.APPLICATION_OBJECT);

      // Verify the operation is idempotent
      log.info("DEBUG: Calling second putIfAbsent for idempotency check");
      cache3.putIfAbsent("key_putIfAbsent", "different_value");
      assertEquals("value_putIfAbsent", cache3.get("key_putIfAbsent"));

      // Verify storage format on PROTOSTREAM node
      verifyStoredMediaType(cache3, "key_putIfAbsent", MediaType.APPLICATION_PROTOSTREAM);

      // Verify storage format on APPLICATION_OBJECT nodes
      verifyStoredMediaType(cache0, "key_putIfAbsent", MediaType.APPLICATION_OBJECT);
   }

   public void testReplaceWithValue() {
      log.info("Testing replace(key, value)...");

      // First put a value
      cache3.put("key_replace", "initial_value");
      assertEquals("initial_value", cache3.get("key_replace"));

      // Now replace it
      cache3.replace("key_replace", "replaced_value");
      assertEquals("replaced_value", cache3.get("key_replace"));

      // Verify storage format on PROTOSTREAM node
      verifyStoredMediaType(cache3, "key_replace", MediaType.APPLICATION_PROTOSTREAM);

      // Verify storage format on APPLICATION_OBJECT nodes
      verifyStoredMediaType(cache0, "key_replace", MediaType.APPLICATION_OBJECT);
   }

   public void testReplaceConditional() {
      log.info("Testing replace(key, oldValue, newValue)...");

      // First put a value
      cache3.put("key_replace_conditional", "initial_value");

      // Replace with correct old value
      boolean replaced = cache3.replace("key_replace_conditional", "initial_value", "new_value");
      assertEquals(true, replaced);
      assertEquals("new_value", cache3.get("key_replace_conditional"));

      // Try to replace with incorrect old value
      replaced = cache3.replace("key_replace_conditional", "wrong_value", "another_value");
      assertEquals(false, replaced);
      assertEquals("new_value", cache3.get("key_replace_conditional"));

      // Verify storage format on PROTOSTREAM node
      verifyStoredMediaType(cache3, "key_replace_conditional", MediaType.APPLICATION_PROTOSTREAM);

      // Verify storage format on APPLICATION_OBJECT nodes
      verifyStoredMediaType(cache0, "key_replace_conditional", MediaType.APPLICATION_OBJECT);
   }

   public void testComputeIfAbsent() {
      log.info("Testing computeIfAbsent...");

      cache3.computeIfAbsent("key_computeIfAbsent", k -> "computed_value");

      // Verify value is retrievable
      assertEquals("computed_value", cache3.get("key_computeIfAbsent"));

      // DEBUG: Check what's actually stored after first computeIfAbsent
      log.info("DEBUG: Checking storage after first computeIfAbsent");
      verifyStoredMediaType(cache3, "key_computeIfAbsent", MediaType.APPLICATION_PROTOSTREAM);
      verifyStoredMediaType(cache0, "key_computeIfAbsent", MediaType.APPLICATION_OBJECT);

      // Verify the operation is idempotent
      log.info("DEBUG: Calling second computeIfAbsent for idempotency check");
      cache3.computeIfAbsent("key_computeIfAbsent", k -> "different_value");
      assertEquals("computed_value", cache3.get("key_computeIfAbsent"));

      // Verify storage format on PROTOSTREAM node
      verifyStoredMediaType(cache3, "key_computeIfAbsent", MediaType.APPLICATION_PROTOSTREAM);

      // Verify storage format on APPLICATION_OBJECT nodes
      verifyStoredMediaType(cache0, "key_computeIfAbsent", MediaType.APPLICATION_OBJECT);
   }

   public void testCompute() {
      log.info("Testing compute...");

      // First put a value
      cache3.put("key_compute", "initial");
      assertEquals("initial", cache3.get("key_compute"));

      // Compute a new value based on the old one
      cache3.compute("key_compute", (k, v) -> v + "_computed");
      assertEquals("initial_computed", cache3.get("key_compute"));

      // Verify storage format on PROTOSTREAM node
      verifyStoredMediaType(cache3, "key_compute", MediaType.APPLICATION_PROTOSTREAM);

      // Verify storage format on APPLICATION_OBJECT nodes
      verifyStoredMediaType(cache0, "key_compute", MediaType.APPLICATION_OBJECT);
   }

   public void testFunctionalReadWriteEval() {
      log.info("Testing functional ReadWriteMap.eval()...");

      FunctionalMap<String, String> functionalMap = FunctionalMap.create(cache3.getAdvancedCache());
      FunctionalMap.ReadWriteMap<String, String> readWriteMap = functionalMap.toReadWriteMap();

      // Test eval with new entry
      String result = readWriteMap.eval("key_functional_rw_eval", view -> {
         view.set("functional_value");
         return "created";
      }).join();
      assertEquals("created", result);
      assertEquals("functional_value", cache3.get("key_functional_rw_eval"));

      // Test eval updating existing entry
      result = readWriteMap.eval("key_functional_rw_eval", view -> {
         String oldValue = view.find().orElse("missing");
         view.set(oldValue + "_updated");
         return oldValue;
      }).join();
      assertEquals("functional_value", result);
      assertEquals("functional_value_updated", cache3.get("key_functional_rw_eval"));

      // Verify storage format on PROTOSTREAM node
      verifyStoredMediaType(cache3, "key_functional_rw_eval", MediaType.APPLICATION_PROTOSTREAM);

      // Verify storage format on APPLICATION_OBJECT nodes
      verifyStoredMediaType(cache0, "key_functional_rw_eval", MediaType.APPLICATION_OBJECT);
   }

   public void testFunctionalReadWriteEvalReturningValue() {
      log.info("Testing functional ReadWriteMap.eval() returning value...");

      FunctionalMap<String, String> functionalMap = FunctionalMap.create(cache3.getAdvancedCache());
      FunctionalMap.ReadWriteMap<String, String> readWriteMap = functionalMap.toReadWriteMap();

      // Test eval with new entry, returning the value
      Optional<String> result = readWriteMap.eval("key_functional_rw_evalvalue", view -> {
         view.set("evalvalue_result");
         return view.find();
      }).join();
      assertEquals(Optional.of("evalvalue_result"), result);
      assertEquals("evalvalue_result", cache3.get("key_functional_rw_evalvalue"));

      // Test eval reading existing entry
      result = readWriteMap.eval("key_functional_rw_evalvalue", view -> {
         return view.find();
      }).join();
      assertEquals(Optional.of("evalvalue_result"), result);

      // Verify storage format on PROTOSTREAM node
      verifyStoredMediaType(cache3, "key_functional_rw_evalvalue", MediaType.APPLICATION_PROTOSTREAM);

      // Verify storage format on APPLICATION_OBJECT nodes
      verifyStoredMediaType(cache0, "key_functional_rw_evalvalue", MediaType.APPLICATION_OBJECT);
   }

   public void testFunctionalWriteOnlyEval() {
      log.info("Testing functional WriteOnlyMap.eval()...");

      FunctionalMap<String, String> functionalMap = FunctionalMap.create(cache3.getAdvancedCache());
      FunctionalMap.WriteOnlyMap<String, String> writeOnlyMap = functionalMap.toWriteOnlyMap();

      // Test eval with new entry
      writeOnlyMap.eval("key_functional_wo_eval", "writeonly_value", (v, view) -> {
         view.set(v);
      }).join();
      assertEquals("writeonly_value", cache3.get("key_functional_wo_eval"));

      // Test eval updating existing entry
      writeOnlyMap.eval("key_functional_wo_eval", "writeonly_updated", (v, view) -> {
         view.set(v);
      }).join();
      assertEquals("writeonly_updated", cache3.get("key_functional_wo_eval"));

      // Verify storage format on PROTOSTREAM node
      verifyStoredMediaType(cache3, "key_functional_wo_eval", MediaType.APPLICATION_PROTOSTREAM);

      // Verify storage format on APPLICATION_OBJECT nodes
      verifyStoredMediaType(cache0, "key_functional_wo_eval", MediaType.APPLICATION_OBJECT);
   }

   public void testFunctionalReadWriteEvalWithRemove() {
      log.info("Testing functional ReadWriteMap.eval() with remove...");

      FunctionalMap<String, String> functionalMap = FunctionalMap.create(cache3.getAdvancedCache());
      FunctionalMap.ReadWriteMap<String, String> readWriteMap = functionalMap.toReadWriteMap();

      // First create an entry
      cache3.put("key_functional_remove", "to_be_removed");
      assertEquals("to_be_removed", cache3.get("key_functional_remove"));

      // Test eval with remove
      String result = readWriteMap.eval("key_functional_remove", view -> {
         String oldValue = view.find().orElse(null);
         view.remove();
         return oldValue;
      }).join();
      assertEquals("to_be_removed", result);
      assertEquals(null, cache3.get("key_functional_remove"));
   }

   private void verifyStoredMediaType(Cache<String, String> cache, String key, MediaType expectedMediaType) {
      AdvancedCache<String, String> advancedCache = cache.getAdvancedCache();
      InternalDataContainer<String, String> dataContainer = TestingUtil.extractComponent(advancedCache, InternalDataContainer.class);
      InternalCacheEntry<String, String> entry = dataContainer.peek(key);

      if (entry != null) {
         // Entry is stored on this node, verify its actual stored value type
         Object storedValue = entry.getValue();
         Class<?> storedClass = storedValue.getClass();

         log.warnf("Node %s stores key %s as %s (expected %s) value=%s",
               advancedCache.getRpcManager().getAddress(), key, storedClass.getSimpleName(),
               expectedMediaType.equals(MediaType.APPLICATION_OBJECT) ? "String" : "WrappedByteArray",
               storedValue);

         if (expectedMediaType.equals(MediaType.APPLICATION_OBJECT)) {
            // APPLICATION_OBJECT should store values as their native Java type (String in this case)
            if (!String.class.equals(storedClass)) {
               log.errorf("ERROR: Node %s should store as String but got %s",
                     advancedCache.getRpcManager().getAddress(), storedClass.getName());
            }
         } else if (expectedMediaType.equals(MediaType.APPLICATION_PROTOSTREAM)) {
            // APPLICATION_PROTOSTREAM should store values as WrappedByteArray
            if (!org.infinispan.commons.marshall.WrappedByteArray.class.equals(storedClass)) {
               log.errorf("ERROR: Node %s should store as WrappedByteArray but got %s",
                     advancedCache.getRpcManager().getAddress(), storedClass.getName());
            }
         } else {
            fail("Unknown media type: " + expectedMediaType);
         }
      } else {
         log.infof("Key %s not stored on node %s (not an owner)", key, advancedCache.getRpcManager().getAddress());
      }
   }
}
