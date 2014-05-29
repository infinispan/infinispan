package org.infinispan.distribution.groups;

import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface in the group's primary owner.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.PrimaryOwnerStateTransferGetGroupKeysTest")
public class PrimaryOwnerStateTransferGetGroupKeysTest extends BaseStateTransferGetGroupKeysTest {

   public PrimaryOwnerStateTransferGetGroupKeysTest() {
      super(TestCacheFactory.PRIMARY_OWNER);
   }
}
