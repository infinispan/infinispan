package org.infinispan.distribution.groups;

import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface in the group's non owner.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.NonOwnerStateTransferGetGroupKeysTest")
public class NonOwnerStateTransferGetGroupKeysTest extends BaseStateTransferGetGroupKeysTest {

   public NonOwnerStateTransferGetGroupKeysTest() {
      super(TestCacheFactory.NON_OWNER);
   }
}
