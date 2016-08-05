package org.infinispan.scattered;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.replication.AsyncAPINonTxSyncReplTest;
import org.infinispan.test.data.Key;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional")
public class AsyncAPINonTxSyncScatteredTest extends AsyncAPINonTxSyncReplTest {
   @Override
   protected ConfigurationBuilder getConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.SCATTERED_SYNC, false);
   }

   @Override
   protected void assertOnAllCaches(Key k, String v, Cache c1, Cache c2) {
      InternalCacheEntry entry1 = c1.getAdvancedCache().getDataContainer().peek(k);
      assertEquals(entry1 == null ? null : entry1.getValue(), v);
      InternalCacheEntry entry2 = c2.getAdvancedCache().getDataContainer().peek(k);
      assertEquals(entry2 == null ? null : entry1.getValue(), v);
   }
}
