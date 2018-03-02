package org.infinispan.distribution.rehash;

import static java.lang.String.format;

import java.util.Arrays;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.distribution.BaseDistStoreTest;
import org.infinispan.distribution.MagicKey;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Should ensure that persistent state is not rehashed if the cache store is shared.  See ISPN-861
 */
@Test (testName = "distribution.rehash.RehashWithSharedStoreTest", groups = "functional")
public class RehashWithSharedStoreTest extends BaseDistStoreTest<Object, String, RehashWithSharedStoreTest> {

   private static final Log log = LogFactory.getLog(RehashWithSharedStoreTest.class);

   @Override
   public Object[] factory() {
      return new Object[] {
         new RehashWithSharedStoreTest().segmented(false),
         new RehashWithSharedStoreTest().segmented(true),
         new RehashWithSharedStoreTest().segmented(false).numOwners(1).l1(false).cacheMode(CacheMode.SCATTERED_SYNC).transactional(false),
         new RehashWithSharedStoreTest().segmented(true).numOwners(1).l1(false).cacheMode(CacheMode.SCATTERED_SYNC).transactional(false),
      };
   }

   public RehashWithSharedStoreTest() {
      INIT_CLUSTER_SIZE = 3;
      testRetVals = true;
      performRehashing = true;
      shared = true;
   }

   @BeforeMethod
   public void afterMethod() {
      clearStats(c1);
   }

   public void testRehashes() throws PersistenceException {
      MagicKey k = new MagicKey("k", c1);

      c1.put(k, "v");

      Cache<Object, String>[] owners = getOwners(k);
      log.infof("Initial owners list for key %s: %s", k, Arrays.asList(owners));

      // Ensure the loader is shared!
      for (Cache<Object, String> c: Arrays.asList(c1, c2, c3)) {
         assert TestingUtil.getFirstLoader(c).contains(k) : format("CacheStore on %s should contain key %s", c, k);
      }

      Cache<Object, String> primaryOwner = owners[0];
      if (getCacheStoreStats(primaryOwner, "write") == 0) primaryOwner = owners[1];

      for (Cache<Object, String> c: owners) {
         int numWrites = getCacheStoreStats(c, "write");
         assert numWrites == 1 : "store() should have been invoked on the cache store once.  Was " + numWrites;
      }

      log.infof("Stopping node %s", primaryOwner);

      caches.remove(primaryOwner);
      primaryOwner.stop();
      primaryOwner.getCacheManager().stop();


      TestingUtil.blockUntilViewsReceived(60000, false, caches);
      TestingUtil.waitForNoRebalance(caches);


      owners = getOwners(k);

      log.infof("After shutting one node down, owners list for key %s: %s", k, Arrays.asList(owners));

      assert owners.length == numOwners;

      for (Cache<Object, String> o : owners) {
         int numWrites = getCacheStoreStats(o, "write");
         assert numWrites == 1 : "store() should have been invoked on the cache store once.  Was " + numWrites;
         assert "v".equals(o.get(k)) : "Should be able to see key on new owner";
      }
   }
}
