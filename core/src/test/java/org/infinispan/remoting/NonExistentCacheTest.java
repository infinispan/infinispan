package org.infinispan.remoting;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.NamedCacheNotFoundException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

import static org.junit.Assert.*;

@Test (testName = "remoting.NonExistentCacheTest", groups = "functional")
public class NonExistentCacheTest extends AbstractInfinispanTest {

   public void testStrictPeerToPeer() {
      doTest(true);
   }

   public void testNonStrictPeerToPeer() {
      doTest(false);
   }

   private EmbeddedCacheManager createCacheManager(boolean strictPeerToPeer) {
      ConfigurationBuilder c = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      c.clustering().cacheMode(CacheMode.REPL_SYNC)
            .transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);

      GlobalConfigurationBuilder gc = GlobalConfigurationBuilder.defaultClusteredBuilder();
      gc.transport().strictPeerToPeer(strictPeerToPeer);

      return TestCacheManagerFactory.createClusteredCacheManager(gc, c);
   }

   private void doTest(boolean strict) {
      EmbeddedCacheManager cm1 = null, cm2 = null;
      try {
         cm1 = createCacheManager(strict);
         cm2 = createCacheManager(strict);

         cm1.getCache();
         cm2.getCache();

         cm1.getCache().put("k", "v");
         assertEquals("v", cm1.getCache().get("k"));
         assertEquals("v", cm2.getCache().get("k"));

         cm1.defineConfiguration("newCache", cm1.getDefaultCacheConfiguration());

         if (strict) {
            try {
               cm1.getCache("newCache").put("k", "v");
               fail("Should have failed!");
            } catch (CacheException e) {
               assertTrue(e.getCause() instanceof NamedCacheNotFoundException);
            }
         } else {
            cm1.getCache("newCache").put("k", "v");
            assertEquals("v", cm1.getCache("newCache").get("k"));
         }
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

}
