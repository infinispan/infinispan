package org.infinispan.tx;

import static org.testng.AssertJUnit.assertFalse;

import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.impl.TxInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.NonTxCacheInterceptorTest")
public class NonTxCacheInterceptorTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(getDefaultStandaloneCacheConfig(false));
   }

   public void testNoTxInterceptor() {
      final AsyncInterceptorChain interceptorChain = cache.getAdvancedCache().getAsyncInterceptorChain();
      log.trace(interceptorChain);
      assertFalse(interceptorChain.containsInterceptorType(TxInterceptor.class));
   }
}
