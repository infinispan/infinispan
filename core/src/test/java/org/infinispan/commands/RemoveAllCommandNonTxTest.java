package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.Future;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateGlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "commands.RemoveAllCommandNonTxTest")
@CleanupAfterMethod
public class RemoveAllCommandNonTxTest extends MultipleCacheManagersTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new RemoveAllCommandNonTxTest().cacheMode(CacheMode.DIST_SYNC),
            new RemoveAllCommandNonTxTest().cacheMode(CacheMode.DIST_SYNC).useTriangle(false),
      };
   }

   @Override
   protected void createCacheManagers() {
      GlobalConfigurationBuilder gcb = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gcb.serialization().addContextInitializer(TestDataSCI.INSTANCE);
      if (useTriangle == Boolean.FALSE) {
         gcb.addModule(PrivateGlobalConfigurationBuilder.class).serverMode(true);
      }
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(cacheMode, false);
      dcc.clustering().hash().numOwners(3).l1().disable();
      createCluster(gcb, dcc, 3);
      waitForClusterToForm();
   }

   public void testRemoveAllSyncOnPrimaryOwner() throws Exception {
      testRemoveAll(true, true);
   }

   public void testRemoveAllAsyncOnPrimaryOwner() throws Exception {
      testRemoveAll(false, true);
   }

   public void testRemoveAllSyncOnBackupOwner() throws Exception {
      testRemoveAll(true, false);
   }

   public void testRemoveAllAsyncOnBackupOwner() throws Exception {
      testRemoveAll(false, false);
   }

   private void testRemoveAll(boolean sync, boolean removeFromPrimary) throws Exception {
      MagicKey key = new MagicKey("key", cache(0));
      cache(0).put(key, "value");

      assertEquals("value", cache(0).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
      assertEquals("value", cache(1).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
      assertEquals("value", cache(2).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));

      int cacheIndex = removeFromPrimary ? 0 : 1;
      if (sync) {
         cache(cacheIndex).removeAll(Collections.singleton(key));
      } else {
         Future<Void> f = cache(cacheIndex).removeAllAsync(Collections.singleton(key));
         assertNotNull(f);
         assertNull(f.get());
         assertTrue(f.isDone());
         assertFalse(f.isCancelled());
      }

      assertNull(cache(0).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
      assertNull(cache(1).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
      assertNull(cache(2).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
   }

   public void testRemoveAllMultipleKeys() {
      MagicKey key1 = new MagicKey("key1", cache(0));
      MagicKey key2 = new MagicKey("key2", cache(1));
      MagicKey key3 = new MagicKey("key3", cache(2));

      cache(0).put(key1, "value1");
      cache(0).put(key2, "value2");
      cache(0).put(key3, "value3");

      cache(1).removeAll(Set.of(key1, key2, key3));

      assertNull(cache(0).get(key1));
      assertNull(cache(0).get(key2));
      assertNull(cache(0).get(key3));
      assertNull(cache(1).get(key1));
      assertNull(cache(1).get(key2));
      assertNull(cache(1).get(key3));
      assertNull(cache(2).get(key1));
      assertNull(cache(2).get(key2));
      assertNull(cache(2).get(key3));
   }
}
