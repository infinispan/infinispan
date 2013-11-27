package org.infinispan.distribution;

import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.DistSyncL1RepeatableReadFuncTest")
public class DistSyncL1RepeatableReadFuncTest extends DistSyncL1FuncTest {
   public DistSyncL1RepeatableReadFuncTest() {
      super();
      isolationLevel = IsolationLevel.REPEATABLE_READ;
   }
}
