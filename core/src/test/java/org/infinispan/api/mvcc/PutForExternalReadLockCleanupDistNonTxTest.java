package org.infinispan.api.mvcc;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.mvcc.PutForExternalReadLockCleanupDistNonTxTest")
@CleanupAfterMethod
public class PutForExternalReadLockCleanupDistNonTxTest extends PutForExternalReadLockCleanupTest {


   @Override
   protected final boolean transactional() {
      return false;
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder builder) {
      //no-op
   }
}
