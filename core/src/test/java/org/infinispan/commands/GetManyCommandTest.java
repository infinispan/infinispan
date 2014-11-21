package org.infinispan.commands;

import static org.testng.Assert.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional")
public abstract class GetManyCommandTest extends MultipleCacheManagersTest {

   private final CacheMode cacheMode;
   private final boolean transactional;
   private final int numNodes = 4;
   private final int numEntries = 100;

   protected GetManyCommandTest(CacheMode cacheMode, boolean transactional) {
      this.cacheMode = cacheMode;
      this.transactional = transactional;
   }

   @Test(groups = "functional")
   public static class DistNonTx extends GetManyCommandTest {
      protected DistNonTx() {
         super(CacheMode.DIST_SYNC, false);
      }
   }

   @Test(groups = "functional")
   public static class DistTx extends GetManyCommandTest {
      protected DistTx() {
         super(CacheMode.DIST_SYNC, true);
      }
   }

   @Test(groups = "functional")
   public static class ReplNonTx extends GetManyCommandTest {
      protected ReplNonTx() {
         super(CacheMode.REPL_SYNC, false);
      }
   }

   @Test(groups = "functional")
   public static class ReplTx extends GetManyCommandTest {
      protected ReplTx() {
         super(CacheMode.REPL_SYNC, true);
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(cacheMode, transactional);
      createCluster(dcc, numNodes);
      waitForClusterToForm();
   }

   public void testGetMany() {
      for (int i = 0; i < numEntries; ++i)
         advancedCache(i % numNodes).put("key" + i, "value" + i);
      for (int i = 0; i < numEntries; ++i)
         for (Cache<Object, Object> cache : caches())
            assertEquals(cache.get("key" + i), "value" + i);

      for (int j = 0; j < 10; ++j) {
         Set<Object> mutableKeys = new HashSet<>();
         Map<Object, Object> expected = new HashMap<>();
         for (int i = j; i < numEntries; i += 10) {
            mutableKeys.add("key" + i);
            expected.put("key" + i, "value" + i);
         }
         Set<Object> immutableKeys = Collections.unmodifiableSet(mutableKeys);

         for (Cache<Object, Object> cache : caches()) {
            Map<Object, Object> result = cache.getMany(immutableKeys);
            assertEquals(result, expected);
         }
      }
   }
}
