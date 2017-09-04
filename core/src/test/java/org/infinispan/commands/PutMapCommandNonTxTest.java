package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.concurrent.Future;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.BiasAcquisition;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.PutMapCommandNonTxTest")
@CleanupAfterMethod
public class PutMapCommandNonTxTest extends MultipleCacheManagersTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new PutMapCommandNonTxTest().cacheMode(CacheMode.DIST_SYNC),
            new PutMapCommandNonTxTest().cacheMode(CacheMode.SCATTERED_SYNC).biasAcquisition(BiasAcquisition.NEVER),
            new PutMapCommandNonTxTest().cacheMode(CacheMode.SCATTERED_SYNC).biasAcquisition(BiasAcquisition.ON_WRITE),
      };
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(cacheMode, false);
      if (!cacheMode.isScattered()) {
         dcc.clustering().hash().numOwners(3).l1().disable();
      }
      if (biasAcquisition != null) {
         dcc.clustering().biasAcquisition(biasAcquisition);
      }
      createCluster(dcc, 3);
      waitForClusterToForm();
   }

   public void testPutMapCommandSyncOnPrimaryOwner() throws Exception {
      testPutMapCommand(true, true);
   }

   public void testPutMapCommandAsyncOnPrimaryOwner() throws Exception {
      testPutMapCommand(false, true);
   }

   public void testPutMapCommandSyncOnBackupOwner() throws Exception {
      testPutMapCommand(true, false);
   }

   public void testPutMapCommandAsyncOnBackupOwner() throws Exception {
      testPutMapCommand(false, false);
   }

   private void testPutMapCommand(boolean sync, boolean putOnPrimary) throws Exception {
      MagicKey key = new MagicKey("key", cache(0));

      if (sync) {
         cache(putOnPrimary ? 0 : 1).putAll(Collections.singletonMap(key, "value"));
      } else {
         Future<Void> f = cache(putOnPrimary ? 0 : 1).putAllAsync(Collections.singletonMap(key, "value"));
         assertNotNull(f);
         assertNull(f.get());
         assertTrue(f.isDone());
         assertFalse(f.isCancelled());
      }

      if (cacheMode.isScattered()) {
         int hasValue = 0;
         for (Cache c : caches()) {
            Object value = c.getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP, Flag.SKIP_OWNERSHIP_CHECK).get(key);
            if ("value".equals(value)) {
               hasValue++;
            } else assertNull(value);
         }
         assertEquals(2, hasValue);
      } else {
         assertEquals("value", cache(0).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
         assertEquals("value", cache(1).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
         assertEquals("value", cache(2).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
      }
   }
}
