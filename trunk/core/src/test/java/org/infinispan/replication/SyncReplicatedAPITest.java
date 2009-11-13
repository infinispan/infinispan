package org.infinispan.replication;

import org.testng.annotations.Test;

@Test(groups = "functional", testName = "replication.SyncReplicatedAPITest")
public class SyncReplicatedAPITest extends BaseReplicatedAPITest {
   public SyncReplicatedAPITest() {
      isSync = true;
   }
}

