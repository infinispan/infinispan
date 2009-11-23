package org.infinispan.api;

import org.infinispan.Cache;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.testng.annotations.Test;

/**
 * This is related to  https://jira.jboss.org/jira/browse/ISPN-83
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "api.NoFlushFailureTest")
public class NoFlushFailureTest {

   CacheManager cm1;
   CacheManager cm2;
   private static final String FILE = "configs/no-flush.xml";
   private Cache<Object,Object> c1;
   private Cache<Object,Object> c2;

   @Test
   public void simpleTest() throws Exception {
      cm1 = new DefaultCacheManager(FILE);
      try {
         cm1.getCache();
         assert false : "Exception expected!";
      } catch (Exception e) {
         //expected
      }
   }
}


