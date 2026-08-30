package org.infinispan.functional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Map;
import java.util.Set;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.functional.FunctionalMap.ReadWriteMap;
import org.infinispan.functional.FunctionalMap.WriteOnlyMap;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.testng.annotations.Test;

/**
 * Verifies that functional commands executed on a non-owner node in DIST_SYNC
 * do not throw ClassCastException when statistics are enabled.
 *
 * @see <a href="https://github.com/infinispan/infinispan/issues/17946">#17946</a>
 */
@Test(groups = "functional", testName = "functional.FunctionalStatsNonOwnerTest")
public class FunctionalStatsNonOwnerTest extends MultipleCacheManagersTest {
   private ReadWriteMap<Object, Object> rwNonOwner;
   private WriteOnlyMap<Object, Object> woNonOwner;
   private MagicKey key;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      builder.clustering().hash().numOwners(1);
      builder.statistics().enable();
      createCluster(TestDataSCI.INSTANCE, builder, 3);
      waitForClusterToForm();

      key = new MagicKey(cache(0));
      FunctionalMap<Object, Object> fmapNonOwner = FunctionalMap.create(cache(2).getAdvancedCache());
      rwNonOwner = fmapNonOwner.toReadWriteMap();
      woNonOwner = fmapNonOwner.toWriteOnlyMap();
   }

   public void testWriteOnlyKeyFromNonOwner() {
      woNonOwner.eval(key, wo -> wo.set("wo-value")).join();
      assertEquals("wo-value", cache(0).get(key));
   }

   public void testWriteOnlyKeyValueFromNonOwner() {
      woNonOwner.eval(key, "wo-kv", (v, wo) -> wo.set(v)).join();
      assertEquals("wo-kv", cache(0).get(key));
   }

   public void testReadWriteKeyFromNonOwner() {
      cache(0).put(key, "existing");
      Object result = rwNonOwner.eval(key, rw -> rw.find().orElse(null)).join();
      assertEquals("existing", result);
   }

   public void testReadWriteKeyValueFromNonOwner() {
      Object result = rwNonOwner.eval(key, "new-val", (v, rw) -> {
         Object prev = rw.find().orElse(null);
         rw.set(v);
         return prev;
      }).join();
      assertNull(result);
      assertEquals("new-val", cache(0).get(key));
   }

   public void testReadWriteManyFromNonOwner() {
      cache(0).put(key, "multi-existing");
      rwNonOwner.evalMany(Set.of(key), rw -> rw.find().orElse(null)).forEach(result ->
            assertEquals("multi-existing", result)
      );
   }

   public void testReadWriteManyEntriesFromNonOwner() {
      rwNonOwner.evalMany(Map.of(key, "multi-val"), (v, rw) -> {
         rw.set(v);
         return v;
      }).forEach(result -> assertEquals("multi-val", result));
      assertEquals("multi-val", cache(0).get(key));
   }
}
