package org.infinispan.query.blackbox;

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.infinispan.query.test.BrokenDocumentId;
import org.infinispan.query.test.BrokenProvided;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * This test is to try and create a searchable cache without the proper annotations used.
 *
 * @author Navin Surtani
 */
@Test(groups = "functional")
public class BrokenAnnotationTest extends SingleCacheManagerTest {
   Cache<?, ?> c;

   public BrokenAnnotationTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testProvided() throws Exception {
      TestQueryHelperFactory.createTestQueryHelperInstance(c, BrokenProvided.class);
   }

   @Test(expectedExceptions = IllegalArgumentException.class)
   public void testDocumentId() throws Exception {
      TestQueryHelperFactory.createTestQueryHelperInstance(c, BrokenDocumentId.class);
   }

   protected CacheManager createCacheManager() throws Exception {
      CacheManager cm = TestCacheManagerFactory.createLocalCacheManager();
      c = cm.getCache();
      return cm;
   }
}
