package org.infinispan.api.mvcc.repeatable_read;

import org.infinispan.api.CacheAPITest;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

import javax.transaction.NotSupportedException;
import javax.transaction.SystemException;

@Test(groups = "functional", testName = "api.mvcc.repeatable_read.CacheAPIOptimisticTest")
public class CacheAPIOptimisticTest extends CacheAPITest {
   @Override
   protected IsolationLevel getIsolationLevel() {
      return IsolationLevel.REPEATABLE_READ;
   }
}