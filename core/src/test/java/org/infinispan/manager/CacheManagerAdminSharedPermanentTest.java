package org.infinispan.manager;

import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 9.2
 */

@Test(testName = "manager.CacheManagerAdminSharedPermanentTest", groups = "functional")
@CleanupAfterMethod
public class CacheManagerAdminSharedPermanentTest extends CacheManagerAdminPermanentTest {
   @Override
   protected boolean isShared() {
      return true;
   }
}
