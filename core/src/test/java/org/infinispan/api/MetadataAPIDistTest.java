package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.infinispan.distribution.DistributionTestHelper.getFirstNonOwner;
import static org.infinispan.distribution.DistributionTestHelper.getFirstOwner;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.internal.junit.ArrayAsserts.assertArrayEquals;

@Test(groups = "functional", testName = "api.MetadataAPIDistTest")
public class MetadataAPIDistTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      Equivalence<byte[]> eq = ByteArrayEquivalence.INSTANCE;
      builder.dataContainer().keyEquivalence(eq).valueEquivalence(eq)
             .clustering().hash().numOwners(1);
      createCluster(builder, 2);
   }

   public void testGetCacheEntryNonOwner() {
      byte[] key = {1, 2, 3};
      Cache<byte[], byte[]> owner = getFirstOwner(key, this.<byte[], byte[]>caches());
      Cache<byte[], byte[]> nonOwner = getFirstNonOwner(key, this.<byte[], byte[]>caches());

      owner.put(key, new byte[]{4, 5, 6});
      assertArrayEquals(new byte[]{4, 5, 6}, owner.get(key));

      CacheEntry cacheEntry = nonOwner.getAdvancedCache().getCacheEntry(key);
      assertNotNull(cacheEntry);
      assertArrayEquals(new byte[]{4, 5, 6}, (byte[]) cacheEntry.getValue());
   }

}
