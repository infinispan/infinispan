package org.infinispan.xsite;

import org.testng.annotations.Test;

@Test(groups = "xsite", testName = "xsite.ImplicitBackupCacheStoppedTest")
public class ImplicitBackupCacheStoppedTest extends BackupCacheStoppedTest {
   public ImplicitBackupCacheStoppedTest() {
      implicitBackupCache = true;
   }
}
