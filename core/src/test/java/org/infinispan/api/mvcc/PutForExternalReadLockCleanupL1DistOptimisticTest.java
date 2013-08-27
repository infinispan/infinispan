package org.infinispan.api.mvcc;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadLockCleanupL1DistOptimisticTest")
@CleanupAfterMethod
public class PutForExternalReadLockCleanupL1DistOptimisticTest extends PutForExternalReadLockCleanupDistOptimisticTest {

   @Override
   protected final void amendConfiguration(ConfigurationBuilder builder) {
      super.amendConfiguration(builder);
      builder.clustering().l1().enable();
   }
}
