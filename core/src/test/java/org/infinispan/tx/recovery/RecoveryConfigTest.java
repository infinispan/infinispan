package org.infinispan.tx.recovery;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryInfoKey;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.infinispan.tx.recovery.RecoveryTestUtil.rm;
import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.RecoveryConfigTest")
public class RecoveryConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return new DefaultCacheManager("configs/recovery-enabled-config.xml");
   }

   public void testRecoveryAndAsyncCaches() throws IOException {
      try {
         cacheManager.getCache("withRecoveryAndAsync");
         assert false;
      } catch (Exception e) {
         //expected
      }
   }

   public void testRecoveryWithCacheConfigured() {
      Configuration withRecoveryAndCache = cacheManager.getCache("withRecoveryAndCache").getConfiguration();
      assert withRecoveryAndCache.isTransactionRecoveryEnabled();
      assertEquals(withRecoveryAndCache.getTransactionRecoveryCacheName(), "noRecovery");
      RecoveryManagerImpl recoveryManager = rm(cacheManager.getCache("withRecoveryAndCache"));
      Cache<RecoveryInfoKey,RecoveryAwareRemoteTransaction> preparedTransactions = (Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction>) recoveryManager.getPreparedTransactions();
      assertEquals(preparedTransactions.getName(), "noRecovery");
   }

   public void testRecoveryWithDefaultCache() {
      Configuration recoveryDefaultCache = cacheManager.getCache("withRecoveryDefaultCache").getConfiguration();
      assert recoveryDefaultCache.isTransactionRecoveryEnabled();
      assertEquals(recoveryDefaultCache.getTransactionRecoveryCacheName(), Configuration.RecoveryType.DEFAULT_RECOVERY_INFO_CACHE);
      RecoveryManagerImpl recoveryManager = rm(cacheManager.getCache("withRecoveryDefaultCache"));
      Cache<RecoveryInfoKey,RecoveryAwareRemoteTransaction> preparedTransactions = (Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction>) recoveryManager.getPreparedTransactions();
      assertEquals(preparedTransactions.getName(), Configuration.RecoveryType.DEFAULT_RECOVERY_INFO_CACHE);
   }

   public void testNoRecovery() {
      Configuration noRecovery = cacheManager.getCache("noRecovery").getConfiguration();
      assert !noRecovery.isTransactionRecoveryEnabled();
      assertEquals("someName", noRecovery.getTransactionRecoveryCacheName());
   }
}
