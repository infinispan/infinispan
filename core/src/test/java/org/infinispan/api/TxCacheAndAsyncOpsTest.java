package org.infinispan.api;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "api.TxCacheAndAsyncOpsTest")
public class TxCacheAndAsyncOpsTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final ConfigurationBuilder defaultStandaloneConfig = getDefaultStandaloneCacheConfig(true);
      return TestCacheManagerFactory.createCacheManager(defaultStandaloneConfig);
   }

   public void testAsyncOps() throws Exception {

      CompletableFuture<Object> result = cache.putAsync("k", "v");
      assert result.get() == null;

      result = cache.removeAsync("k");
      assert result.get().equals("v");

      final CompletableFuture<Void> voidNotifyingFuture = cache.putAllAsync(Collections.singletonMap("k", "v"));
      voidNotifyingFuture.get();

      assert cache.get("k").equals("v");
   }
}
