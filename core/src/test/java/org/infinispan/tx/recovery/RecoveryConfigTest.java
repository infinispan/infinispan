package org.infinispan.tx.recovery;

import static org.infinispan.configuration.cache.RecoveryConfiguration.DEFAULT_RECOVERY_INFO_CACHE;
import static org.infinispan.tx.recovery.RecoveryTestUtil.rm;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.infinispan.Cache;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.Combine;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryInfoKey;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.RecoveryConfigTest")
public class RecoveryConfigTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.fromXml("configs/recovery-enabled-config.xml");
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testRecoveryAndAsyncCaches() {
      //Note: this configuration uses Xa Enlistment (see configs/recovery-enabled-config.xml).
      Configuration defaultConfig = cacheManager.getDefaultCacheConfiguration();
      ConfigurationBuilder builder = new ConfigurationBuilder().read(defaultConfig, Combine.DEFAULT);
      builder.clustering().cacheMode(CacheMode.REPL_ASYNC);
      builder.transaction().recovery().enable();
      //it should throw an exception when try to build this configuration.
      builder.build();
   }

   public void testRecoveryWithCacheConfigured() {
      Configuration withRecoveryAndCache = cacheManager.getCache("withRecoveryAndCache").getCacheConfiguration();
      assertTrue(withRecoveryAndCache.transaction().recovery().enabled(), "Recovery is supposed to be enabled.");
      assertEquals("noRecovery", withRecoveryAndCache.transaction().recovery().recoveryInfoCacheName(), "Wrong recovery cache name.");
      RecoveryManagerImpl recoveryManager = rm(cacheManager.getCache("withRecoveryAndCache"));
      assertNotNull(recoveryManager, "RecoveryManager should be *not* null when recovery is enabled.");
      Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction> preparedTransactions = (Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction>) recoveryManager.getInDoubtTransactionsMap();
      assertEquals("noRecovery", preparedTransactions.getName(), "Wrong recovery cache name.");
   }

   public void testRecoveryWithDefaultCache() {
      Configuration recoveryDefaultCache = cacheManager.getCache("withRecoveryDefaultCache").getCacheConfiguration();
      assertTrue(recoveryDefaultCache.transaction().recovery().enabled(), "Recovery is supposed to be enabled.");
      assertEquals(DEFAULT_RECOVERY_INFO_CACHE, recoveryDefaultCache.transaction().recovery().recoveryInfoCacheName(), "Wrong recovery cache name.");
      RecoveryManagerImpl recoveryManager = rm(cacheManager.getCache("withRecoveryDefaultCache"));
      assertNotNull(recoveryManager, "RecoveryManager should be *not* null when recovery is enabled.");
      Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction> preparedTransactions = (Cache<RecoveryInfoKey, RecoveryAwareRemoteTransaction>) recoveryManager.getInDoubtTransactionsMap();
      assertEquals(DEFAULT_RECOVERY_INFO_CACHE, preparedTransactions.getName(), "Wrong recovery cache name.");
   }

   public void testNoRecovery() {
      Configuration noRecovery = cacheManager.getCache("noRecovery").getCacheConfiguration();
      assertFalse(noRecovery.transaction().recovery().enabled(), "Recovery is supposed to be disabled");
      assertNull(rm(cacheManager.getCache("noRecovery")), "RecoveryManager should be null when recovery is disabled");
      assertEquals("someName", noRecovery.transaction().recovery().recoveryInfoCacheName(), "Wrong recovery cache name.");
   }
}
