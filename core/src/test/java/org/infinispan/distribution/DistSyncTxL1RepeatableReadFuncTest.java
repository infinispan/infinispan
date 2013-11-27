package org.infinispan.distribution;

import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistSyncTxL1RepeatableReadFuncTest")
public class DistSyncTxL1RepeatableReadFuncTest extends DistSyncTxL1FuncTest {
   public DistSyncTxL1RepeatableReadFuncTest() {
      super();
      isolationLevel = IsolationLevel.REPEATABLE_READ;
   }
}
