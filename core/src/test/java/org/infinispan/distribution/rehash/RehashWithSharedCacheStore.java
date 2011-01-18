package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistCacheStoreTest;
import org.infinispan.distribution.MagicKey;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.dummy.DummyInMemoryCacheStore;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.Arrays;

import static java.lang.String.format;

/**
 * Should ensure that persistent state is not rehashed if the cache store is shared.  See ISPN-861
 *
 */
@Test (testName = "distribution.rehash.RehashWithSharedCacheStore", groups = "functional")
public class RehashWithSharedCacheStore extends BaseDistCacheStoreTest {

   private static final Log log = LogFactory.getLog(RehashWithSharedCacheStore.class);

   public RehashWithSharedCacheStore() {
      INIT_CLUSTER_SIZE = 3;
      sync = true;
      tx = false;
      testRetVals = true;
      shared = true;
   }

   private CacheStore getCacheStore(Cache<?,?> cache) {
      CacheLoaderManager clm = cache.getAdvancedCache().getComponentRegistry().getComponent(CacheLoaderManager.class);
      return clm.getCacheStore();
   }

   private int getCacheStoreStats(Cache<?, ?> cache, String cacheStoreMethod) {
      DummyInMemoryCacheStore cs = (DummyInMemoryCacheStore) getCacheStore(cache);
      return cs.stats().get(cacheStoreMethod);
   }

   public void testRehashes() throws CacheLoaderException {
      MagicKey k = new MagicKey(c1, "k");

      c1.put(k, "v");

      Cache<Object, String>[] owners = getOwners(k);
      log.info("Initial owners list for key {0}: {1}", k, Arrays.asList(owners));

      // Ensure the loader is shared!
      for (Cache<Object, String> c: Arrays.asList(c1, c2, c3)) {
         assert getCacheStore(c).containsKey(k) : format("CacheStore on %s should contain key %s", c, k);
      }

      Cache<Object, String> primaryOwner = owners[0];
      if (getCacheStoreStats(primaryOwner, "store") == 0) primaryOwner = owners[1];

      for (Cache<Object, String> c: owners) {
         int numWrites = getCacheStoreStats(c, "store");
         assert numWrites == 1 : "store() should have been invoked on the cache store once.  Was " + numWrites;
      }

      log.info("Stopping node {0}", primaryOwner);

      caches.remove(primaryOwner);
      primaryOwner.stop();
      primaryOwner.getCacheManager().stop();


      RehashWaiter.waitForRehashToComplete(caches.toArray(new Cache[INIT_CLUSTER_SIZE - 1]));



      owners = getOwners(k);

      log.info("After shutting one node down, owners list for key {0}: {1}", k, Arrays.asList(owners));

      assert owners.length == 2;

      for (Cache<Object, String> o : owners) {
         int numWrites = getCacheStoreStats(o, "store");
         assert numWrites == 1 : "store() should have been invoked on the cache store once.  Was " + numWrites;
         assert "v".equals(o.get(k)) : "Should be able to see key on new owner";
      }
   }
}
