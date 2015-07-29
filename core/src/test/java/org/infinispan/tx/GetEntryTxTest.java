package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import static org.junit.Assert.assertNotNull;

/**
 * Tests for getCacheEntry under Tx
 *
 * @author gustavonalle
 * @since 8.0
 */
@Test(groups = "functional", testName = "tx.GetEntryTxTest")
public class GetEntryTxTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.clustering().hash().numOwners(1);
      createCluster(builder, 2);
      waitForClusterToForm();
   }

   public void testGetEntry() throws Exception {
      Cache<MagicKey, String> cache = cache(0);

      MagicKey localKey = new MagicKey(cache(0));
      MagicKey remoteKey = new MagicKey(cache(1));

      cache.put(localKey, localKey.toString());
      cache.put(remoteKey, remoteKey.toString());

      assertNotNull(cache.getAdvancedCache().getCacheEntry(localKey));
      assertNotNull(cache.getAdvancedCache().getCacheEntry(remoteKey));
   }

}
