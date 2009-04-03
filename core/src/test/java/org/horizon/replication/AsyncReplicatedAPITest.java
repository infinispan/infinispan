package org.horizon.replication;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "replication.AsyncReplicatedAPITest")
public class AsyncReplicatedAPITest extends BaseReplicatedAPITest {
   public AsyncReplicatedAPITest() {
      isSync = false;
   }
}
