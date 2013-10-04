package org.infinispan.query.config;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.CacheContainer;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.config.CacheModeTest")
public class CacheModeTest extends AbstractInfinispanTest {

   public void testLocal() {
      doTest(CacheMode.LOCAL);
   }

   public void testReplicated() {
      doTest(CacheMode.REPL_SYNC);
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN(\\d)*: Indexing can not be enabled on caches in Invalidation mode")
   public void testInvalidated() {
      doTest(CacheMode.INVALIDATION_SYNC);
   }

   public void testDistributed() {
      doTest(CacheMode.DIST_SYNC);
   }

   private void doTest(CacheMode m) {
      CacheContainer cc = null;
      try {
         cc = TestCacheManagerFactory.createCacheManager(m, true);
         boolean found = false;
         for (CommandInterceptor i : cc.getCache().getAdvancedCache().getInterceptorChain()) {
            if (i instanceof QueryInterceptor) found = true;
         }
         assert found : "Didn't find a query interceptor in the chain!!";
      } finally {
         TestingUtil.killCacheManagers(cc);
      }
   }
}
