package org.infinispan.api.mvcc;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadLockCleanupL1DistNonTxTest")
@CleanupAfterMethod
public class PutForExternalReadLockCleanupL1DistNonTxTest extends PutForExternalReadLockCleanupDistNonTxTest {

   @Override
   protected final void amendConfiguration(ConfigurationBuilder builder) {
      builder.clustering().l1().enable();
   }
}
