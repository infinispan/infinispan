package org.infinispan.api;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import jakarta.transaction.TransactionManager;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "api.AsyncWithTxTest")
public class AsyncWithTxTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfig = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      addClusterEnabledCacheManager(defaultConfig);
      addClusterEnabledCacheManager(defaultConfig);
   }

   public void testWithTx() throws Exception {
      TransactionManager transactionManager = TestingUtil.getTransactionManager(cache(0));
      cache(0).put("k","v1");
      transactionManager.begin();
      CompletableFuture<Object> future = cache(0).putAsync("k", "v2");
      "v1".equals(future.get(2000, TimeUnit.MILLISECONDS));
      transactionManager.commit();
   }
}
