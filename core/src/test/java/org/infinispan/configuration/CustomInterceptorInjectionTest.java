package org.infinispan.configuration;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.BaseCustomAsyncInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

/**
 * Test that injection in interceptors works as expected.
 *
 * @author Mircea Markus
 * @since 5.0
 */
@Test(groups = "functional", testName = "configuration.CustomInterceptorInjectionTest")
public class CustomInterceptorInjectionTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = getDefaultStandaloneCacheConfig(false);
      builder.customInterceptors().addInterceptor().index(0).interceptor(new SomeAsyncInterceptor());
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testBaseCustomAsyncInterceptorInjection() {
      AsyncInterceptor interceptor = cache.getAdvancedCache().getAsyncInterceptorChain().getInterceptors().get(0);
      assertEquals(SomeAsyncInterceptor.class, interceptor.getClass());

      SomeAsyncInterceptor someAsyncInterceptor = (SomeAsyncInterceptor) interceptor;
      assertSame(cache.getAdvancedCache().getLockManager(), someAsyncInterceptor.lm);
      assertSame(cache.getAdvancedCache().getDataContainer(), someAsyncInterceptor.dc);
   }

   static class SomeAsyncInterceptor extends BaseCustomAsyncInterceptor {

      @Inject
      LockManager lm;

      DataContainer dc;

      @Override
      protected void start() {
         dc = cache.getAdvancedCache().getDataContainer();
      }
   }
}
