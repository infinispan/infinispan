package org.infinispan.distribution.rehash;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistStoreTest;
import org.infinispan.distribution.MagicKey;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
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
         DummyInMemoryStore dims = TestingUtil.getFirstStore(c);
         assertTrue("CacheStore on " + c + " should contain key " + k, dims.contains(k));
      }

      Cache<Object, String> primaryOwner = owners[0];
      if (getCacheStoreStats(primaryOwner, "write") == 0) primaryOwner = owners[1];

      for (Cache<Object, String> c: owners) {
         int numWrites = getCacheStoreStats(c, "write");
         assertEquals(1, numWrites);
      }

      log.infof("Stopping node %s", primaryOwner);

      caches.remove(primaryOwner);
      primaryOwner.stop();
      primaryOwner.getCacheManager().stop();


      TestingUtil.blockUntilViewsReceived(60000, false, caches);
      TestingUtil.waitForNoRebalance(caches);


      owners = getOwners(k);

      log.infof("After shutting one node down, owners list for key %s: %s", k, Arrays.asList(owners));

      assertEquals(numOwners, owners.length);

      for (Cache<Object, String> o : owners) {
         int numWrites = getCacheStoreStats(o, "write");
         assertEquals(1, numWrites);
         assertEquals("v", o.get(k));
      }
   }
}
