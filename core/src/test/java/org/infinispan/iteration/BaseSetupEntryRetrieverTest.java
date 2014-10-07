package org.infinispan.iteration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Base class used solely for setting up cluster configuration for use with entry iterators
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.BaseSetupEntryRetrieverTest")
public abstract class BaseSetupEntryRetrieverTest extends MultipleCacheManagersTest {
   protected final String CACHE_NAME = getClass().getName();
   protected ConfigurationBuilder builderUsed;
   protected final boolean tx;
   protected final CacheMode cacheMode;

   public BaseSetupEntryRetrieverTest(boolean tx, CacheMode mode) {
      this.tx = tx;
      cacheMode = mode;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      builderUsed = new ConfigurationBuilder();
      builderUsed.clustering().cacheMode(cacheMode);
      if (tx) {
         builderUsed.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      }
      if (cacheMode.isClustered()) {
         builderUsed.clustering().hash().numOwners(2);
         builderUsed.clustering().stateTransfer().chunkSize(50);
         createClusteredCaches(3, CACHE_NAME, builderUsed);
      } else {
         EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builderUsed);
         cacheManagers.add(cm);
      }
   }

   protected static <K, V> Map<K, V> mapFromIterator(Iterator<CacheEntry<K, V>> iterator) {
      Map<K, V> map = new HashMap<K, V>();
      while (iterator.hasNext()) {
         Map.Entry<K, V> entry = iterator.next();
         map.put(entry.getKey(), entry.getValue());
      }
      return map;
   }

   protected static <K, V> Map<K, V> mapFromIterable(Iterable<CacheEntry<K, V>> iterable) {
      Map<K, V> map = new HashMap<K, V>();
      for (CacheEntry<K, V> entry : iterable) {
         map.put(entry.getKey(), entry.getValue());
      }
      return map;
   }
}
