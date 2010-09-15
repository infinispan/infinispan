package org.infinispan.remoting;

import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.config.GlobalConfiguration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.NamedCacheNotFoundException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test (testName = "remoting.NonExistentCacheTest", groups = "functional")
public class NonExistentCacheTest extends AbstractInfinispanTest {

   public void testStrictPeerToPeer() {
      doTest(true);
   }

   public void testNonStrictPeerToPeer() {
      doTest(false);
   }

   private void doTest(boolean strict) {
      EmbeddedCacheManager cm1 = null, cm2 = null;
      try {
         Configuration c = new Configuration();
         c.setCacheMode(Configuration.CacheMode.REPL_SYNC);
         GlobalConfiguration gc = GlobalConfiguration.getClusteredDefault();
         gc.setStrictPeerToPeer(strict);

         cm1 = TestCacheManagerFactory.createCacheManager(gc, c);
         cm2 = TestCacheManagerFactory.createCacheManager(gc, c);

         cm1.getCache();
         cm2.getCache();

         cm1.getCache().put("k", "v");
         assert "v".equals(cm1.getCache().get("k"));
         assert "v".equals(cm2.getCache().get("k"));

         cm1.defineConfiguration("newCache", c);

         if (strict) {
            try {
               cm1.getCache("newCache").put("k", "v");
               assert false : "Should have failed!";
            } catch (CacheException e) {
               assert e.getCause() instanceof NamedCacheNotFoundException;
            }
         } else {
            cm1.getCache("newCache").put("k", "v");
            assert "v".equals(cm1.getCache("newCache").get("k"));
         }
      } finally {
         TestingUtil.killCacheManagers(cm1, cm2);
      }
   }

}
