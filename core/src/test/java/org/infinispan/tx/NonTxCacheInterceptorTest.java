package org.infinispan.tx;

import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.List;

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
      final List<CommandInterceptor> interceptorChain = cache.getAdvancedCache().getInterceptorChain();
      log.trace(interceptorChain);
      for (CommandInterceptor ci : interceptorChain) {
         assert !ci.getClass().equals(TxInterceptor.class);
      }
   }
}
