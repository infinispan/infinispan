package org.infinispan.api;

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.NotifyingFuture;
import org.testng.annotations.Test;

import java.util.Collections;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "api.TxCacheAndAsyncOpsTest")
public class TxCacheAndAsyncOpsTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final Configuration defaultStandaloneConfig = getDefaultStandaloneConfig(true);
      return TestCacheManagerFactory.createCacheManager(defaultStandaloneConfig);
   }

   public void testAsyncOps() throws Exception {

      NotifyingFuture<Object> result = cache.putAsync("k", "v");
      assert result.get() == null;

      result = cache.removeAsync("k");
      assert result.get().equals("v");

      final NotifyingFuture<Void> voidNotifyingFuture = cache.putAllAsync(Collections.singletonMap("k", "v"));
      voidNotifyingFuture.get();

      assert cache.get("k").equals("v");
   }
}
