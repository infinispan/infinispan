package org.infinispan.spring.embedded.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.springframework.cache.Cache;
import org.testng.annotations.Test;

/**
 * Verifies the behaviour of {@link SpringEmbeddedCache} with an invalidation cache, i.e. that values loaded on a
 * cache miss ({@code @Cacheable(sync = true)}) do not invalidate other nodes when {@code putForExternalReadOnLoad}
 * is enabled, while explicit puts and evictions still do.
 */
@Test(groups = "functional", testName = "spring.embedded.provider.SpringEmbeddedCacheInvalidationTest")
public class SpringEmbeddedCacheInvalidationTest extends MultipleCacheManagersTest {

   private static final String CACHE = "invalidation";
   private static final String OTHER_CACHE = "invalidation-other";

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.INVALIDATION_SYNC);
      createClusteredCaches(2, CACHE, builder);
      defineConfigurationOnAllManagers(OTHER_CACHE, builder);
      waitForClusterToForm(OTHER_CACHE);
   }

   private Cache springCache(int node, boolean putForExternalReadOnLoad) {
      return new SpringEmbeddedCacheManager(manager(node), false, putForExternalReadOnLoad).getCache(CACHE);
   }

   public void testLoadDoesNotInvalidateOtherNodesWhenEnabled() {
      Cache node0 = springCache(0, true);
      Cache node1 = springCache(1, true);

      assertEquals("v0", node0.get("k", () -> "v0"));
      assertEquals("v1", node1.get("k", () -> "v1"));

      // node0 keeps its entry: the load on node1 did not invalidate it
      assertEquals("v0", cache(0, CACHE).get("k"));
      assertEquals("v1", cache(1, CACHE).get("k"));
   }

   public void testLoadInvalidatesOtherNodesWhenDisabled() {
      Cache node0 = springCache(0, false);
      Cache node1 = springCache(1, false);

      assertEquals("v0", node0.get("k2", () -> "v0"));
      assertEquals("v1", node1.get("k2", () -> "v1"));

      // default behaviour: putIfAbsent on node1 invalidates node0
      assertNull(cache(0, CACHE).get("k2"));
      assertEquals("v1", cache(1, CACHE).get("k2"));
   }

   public void testPutAndEvictStillInvalidateWhenEnabled() {
      Cache node0 = springCache(0, true);
      Cache node1 = springCache(1, true);

      node0.get("k3", () -> "v0");
      node1.put("k3", "v1"); // @CachePut
      assertNull(cache(0, CACHE).get("k3"));
      assertEquals("v1", cache(1, CACHE).get("k3"));

      node0.get("k3", () -> "v0");
      node1.evict("k3"); // @CacheEvict
      assertNull(cache(0, CACHE).get("k3"));
      assertNull(cache(1, CACHE).get("k3"));
   }

   public void testEnabledOnlyForSelectedCaches() {
      SpringEmbeddedCacheManager manager0 = new SpringEmbeddedCacheManager(manager(0), false, CACHE::equals);
      SpringEmbeddedCacheManager manager1 = new SpringEmbeddedCacheManager(manager(1), false, CACHE::equals);

      manager0.getCache(CACHE).get("k5", () -> "v0");
      manager1.getCache(CACHE).get("k5", () -> "v1");
      assertEquals("v0", cache(0, CACHE).get("k5"));

      manager0.getCache(OTHER_CACHE).get("k5", () -> "v0");
      manager1.getCache(OTHER_CACHE).get("k5", () -> "v1");
      assertNull(cache(0, OTHER_CACHE).get("k5"));
      assertEquals("v1", cache(1, OTHER_CACHE).get("k5"));
   }

   public void testExistingEntryIsNotOverwrittenOnLoad() {
      Cache node0 = springCache(0, true);
      cache(0, CACHE).putForExternalRead("k4", "existing");
      assertEquals("existing", node0.get("k4", () -> "loaded"));
   }
}
