package org.infinispan.stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

import java.util.Iterator;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * Test to verify stream behavior for a replicated cache
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.ReplicatedStreamIteratorTest")
public class ReplicatedStreamIteratorTest extends BaseClusteredStreamIteratorTest {

   public ReplicatedStreamIteratorTest() {
      super(false, CacheMode.REPL_SYNC);
   }

   @Override
   protected Object getKeyTiedToCache(Cache<?, ?> cache) {
      return new MagicKey(cache);
   }

   @Override
   protected void afterCacheCreated(ConfigurationBuilder builder) {
      GlobalConfigurationBuilder global = defaultGlobalConfigurationBuilder();
      global.zeroCapacityNode(true);
      global.serialization().addContextInitializer(sci);
      createClusteredCaches(1, global, builder, false, new TransportFlags().withFD(true), CACHE_NAME);
   }

   public void testIterateFromZeroCapacityNode() {
      Cache<MagicKey, String> cacheZero = cache(getZeroCapacityIndex(), CACHE_NAME);
      Cache<MagicKey, String> cacheNonZero = null;
      for (int i = 0; i < managers().length; i++) {
         EmbeddedCacheManager ecm = manager(i);
         if (!ecm.getCacheManagerConfiguration().isZeroCapacityNode()) {
            cacheNonZero = cache(i, CACHE_NAME);
            break;
         }
      }

      assertThat(cacheNonZero).isNotNull();

      Map<Object, String> values = putValuesInCache();

      Iterator<CacheEntry<MagicKey, String>> iteratorNonZero = cacheNonZero.getAdvancedCache().cacheEntrySet().stream().iterator();
      Map<MagicKey, String> resultsNonZero = mapFromIterator(iteratorNonZero);
      assertEquals(values, resultsNonZero);

      Iterator<CacheEntry<MagicKey, String>> iteratorZero = cacheZero.getAdvancedCache().cacheEntrySet().stream().iterator();
      Map<MagicKey, String> resultsZero = mapFromIterator(iteratorZero);
      assertEquals(values, resultsZero);

      // Ensure the local version of zero-capacity is empty.
      Iterator<CacheEntry<MagicKey, String>> iteratorEmpty = cacheZero.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).cacheEntrySet().stream().iterator();
      assertThat(iteratorEmpty.hasNext()).isFalse();
   }

   private int getZeroCapacityIndex() {
      for (int i = 0; i < managers().length; i++) {
         if (manager(i).getCacheManagerConfiguration().isZeroCapacityNode())
            return i;
      }

      throw new IllegalStateException("Zero capacity node not defined");
   }
}
