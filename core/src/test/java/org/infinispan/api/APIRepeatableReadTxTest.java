package org.infinispan.api;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author William Burns
 * @since 7.2
 */
@Test (groups = "functional", testName = "api.APIRepeatableReadTxTest")
public class APIRepeatableReadTxTest extends APITxTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // start a single cache instance
      ConfigurationBuilder c = getDefaultStandaloneCacheConfig(true);
      c.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(false);
      cm.defineConfiguration("test", c.build());
      cache = cm.getCache("test");
      return cm;
   }

   @Override
   public void testRetainAllMethodOfEntryCollection() {
      //pruivo.note:
      //write-skew is not stored in ImmortalCacheEntry
      //should we add equals() to MetadataImmortalCacheEntry and re-implement the test using it?
      //TBH, it doesn't make much sense to expose our internal cache entries...
   }
}
