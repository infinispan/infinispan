package org.infinispan.configuration;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test(groups = "functional", testName = "config.CustomInterceptorInjectionTest")
public class CustomInterceptorInjectionTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(false);
      c.customInterceptors().addInterceptor().index(0).interceptor(new SomeInterceptor());
      return TestCacheManagerFactory.createCacheManager(c);
   }

   public void testInjectionWorks() {
      final Cache<Object,Object> cache1 = cacheManager.getCache();
      assert cache1.getAdvancedCache().getInterceptorChain().get(0).getClass().equals(SomeInterceptor.class);
      assert SomeInterceptor.lm != null;
   }
}
