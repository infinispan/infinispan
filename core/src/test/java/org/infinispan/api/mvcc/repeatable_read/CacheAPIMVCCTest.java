package org.infinispan.api.mvcc.repeatable_read;

import org.infinispan.api.CacheAPITest;
import org.infinispan.lock.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.repeatable_read.CacheAPIMVCCTest")
public class CacheAPIMVCCTest extends CacheAPITest {
   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.REPEATABLE_READ;
   }
}