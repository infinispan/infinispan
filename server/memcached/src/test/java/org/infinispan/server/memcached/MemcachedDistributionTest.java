package org.infinispan.server.memcached;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.infinispan.Cache;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;

/**
 * Tests distributed mode with Memcached servers.
 *
 * @author Galder Zamarreño
 * @since 6.0
 */
@Test(groups = "functional", testName = "server.memcached.MemcachedDistributionTest")
public class MemcachedDistributionTest extends MemcachedMultiNodeTest {

   public EmbeddedCacheManager createCacheManager(int index) {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(1)
              .dataContainer().valueEquivalence(ByteArrayEquivalence.INSTANCE);
      return TestCacheManagerFactory.createClusteredCacheManager(builder);
   }

   public void testGetFromNonOwner() throws InterruptedException, ExecutionException, TimeoutException {
      MemcachedClient owner = getFirstOwner("1");
      OperationFuture<Boolean> f = owner.set("1", 0, "v1");
      assertTrue(f.get(timeout, TimeUnit.SECONDS));
      MemcachedClient nonOwner = getFirstNonOwner("1");
      assertEquals(nonOwner.get("1"), "v1");
   }

   private MemcachedClient getFirstNonOwner(String k) {
      return getCacheThat(k, false);
   }

   private MemcachedClient getFirstOwner(String k) {
      return getCacheThat(k, true);
   }

   private MemcachedClient getCacheThat(String k, Boolean owner) {
      List<Cache<String, byte[]>> caches = servers.stream().map(s -> {
         Cache<String, byte[]> cache = s.getCacheManager().getCache(cacheName);
         return cache;
      }).collect(Collectors.toList());
      Cache<String, byte[]> cache;
      if (owner) {
         cache = DistributionTestHelper.getFirstOwner(k, caches);
      } else {
         cache = DistributionTestHelper.getFirstNonOwner(k, caches);
      }
      return cacheClient.get(cache);
   }

}
