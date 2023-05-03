package org.infinispan.commands;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
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

@Test(groups = "functional", testName = "commands.PutMapCommandNonTxTest")
@CleanupAfterMethod
public class PutMapCommandNonTxTest extends MultipleCacheManagersTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new PutMapCommandNonTxTest().cacheMode(CacheMode.DIST_SYNC),
            new PutMapCommandNonTxTest().cacheMode(CacheMode.DIST_SYNC).useTriangle(false),
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

      assertEquals("value", cache(0).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
      assertEquals("value", cache(1).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
      assertEquals("value", cache(2).getAdvancedCache().withFlags(Flag.SKIP_REMOTE_LOOKUP).get(key));
   }
}
