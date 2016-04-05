package org.infinispan.tx;

import org.infinispan.interceptors.SequentialInterceptorChain;
import org.infinispan.interceptors.impl.TxInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertFalse;

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
      final SequentialInterceptorChain interceptorChain = cache.getAdvancedCache().getSequentialInterceptorChain();
      log.trace(interceptorChain);
      assertFalse(interceptorChain.containsInterceptorType(TxInterceptor.class));
   }
}
