package org.infinispan.api.mvcc.repeatable_read;

import org.infinispan.api.BaseCacheAPIOptimisticTest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.repeatable_read.CacheAPIOptimisticTest")
public class CacheAPIOptimisticTest extends BaseCacheAPIOptimisticTest {
   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.REPEATABLE_READ;
   }

   @Override
   public void testRetainAllMethodOfEntryCollection() {
      //pruivo.note:
      //write-skew is not stored in ImmortalCacheEntry
      //should we add equals() to MetadataImmortalCacheEntry and re-implement the test using it?
      //TBH, it doesn't make much sense to expose our internal cache entries...
   }
}
