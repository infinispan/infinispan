package org.infinispan.distribution.rehash;

import org.infinispan.Cache;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.distribution.ch.TopologyInfo;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests performing some work on the joiner during a JOIN
 *
 * @author Manik Surtani
 * @since 4.0
 */
// TODO This test makes no sense anymore, now that a joiner blocks until the join completes, before returning from cache.start().  This test needs to be re-thought and redesigned to test the eventual consistency (UnsureResponse) of a remote GET properly.
@Test(groups = "functional", testName = "distribution.rehash.WorkDuringJoinTest", enabled = false)
public class WorkDuringJoinTest extends BaseDistFunctionalTest {

   EmbeddedCacheManager joinerManager;
   Cache<Object, String> joiner;

   public WorkDuringJoinTest() {
      INIT_CLUSTER_SIZE = 2;
   }

   private List<MagicKey> init() {
      List<MagicKey> keys = new ArrayList<MagicKey>(Arrays.asList(
            new MagicKey(c1, "k1"), new MagicKey(c2, "k2"),
            new MagicKey(c1, "k3"), new MagicKey(c2, "k4")
      ));

      int i = 0;
      for (Cache<Object, String> c : caches) c.put(keys.get(i++), "v" + i);

      log.info("Initialized with keys {0}", keys);
      return keys;
   }

   Address startNewMember() {
      joinerManager = addClusterEnabledCacheManager();
      joinerManager.defineConfiguration(cacheName, configuration);
      joiner = joinerManager.getCache(cacheName);
      return manager(joiner).getAddress();
   }

   public void testJoinAndGet() throws ClassNotFoundException, InstantiationException, IllegalAccessException {
      List<MagicKey> keys = init();
      ConsistentHash chOld = getConsistentHash(c1);
      Address joinerAddress = startNewMember();
      ConsistentHash chNew = ConsistentHashHelper.createConsistentHash(chOld.getClass(), chOld.getCaches(), new TopologyInfo(), joinerAddress);
      // which key should me mapped to the joiner?
      MagicKey keyToTest = null;
      for (MagicKey k: keys) {
         if (chNew.isKeyLocalToAddress(joinerAddress, k, numOwners)) {
            keyToTest = k;
            break;
         }
      }

      if (keyToTest == null) throw new NullPointerException("Couldn't find a key mapped to J!");
      assert joiner.get(keyToTest) != null;
   }
}
