package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.PutMapCommandTest")
public class PutMapCommandTest extends MultipleCacheManagersTest {
   protected int numberOfKeys = 10;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().hash().numOwners(1).l1().disable();
      dcc.locking().transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      createCluster(dcc, 4);
      waitForClusterToForm();
   }

   public void testPutOnNonOwner() {  //todo [anistor] this does not test putAll !
      MagicKey mk = new MagicKey("key", cache(0));
      cache(3).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).put(mk, "value");

      assert cache(0).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(mk) != null;
      assert cache(1).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(mk) == null;
      assert cache(2).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(mk) == null;
      assert cache(3).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(mk) == null;
   }

   public void testPutMapCommand() {
      for (int i = 0; i < numberOfKeys; ++i) {
         assert cache(0).get("key" + i) == null;
         assert cache(1).get("key" + i) == null;
         assert cache(2).get("key" + i) == null;
         assert cache(3).get("key" + i) == null;
      }

      Map<String, String> map = new HashMap<>();
      for (int i = 0; i < numberOfKeys; ++i) {
         map.put("key" + i, "value" + i);
      }

      cache(0).putAll(map);

      for (int i = 0; i < numberOfKeys; ++i) {
         assertEquals("value" + i, cache(0).get("key" + i));
         final int finalI = i;
         eventuallyEquals("value" + i, () -> cache(1).get("key" + finalI));
         eventuallyEquals("value" + i, () -> cache(2).get("key" + finalI));
         eventuallyEquals("value" + i, () -> cache(3).get("key" + finalI));
      }
   }
}
