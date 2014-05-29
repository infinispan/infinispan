package org.infinispan.distribution.groups;

import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface in the group's backup owner.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.BackupOwnerStateTransferGetGroupKeysTest")
public class BackupOwnerStateTransferGetGroupKeysTest extends BaseStateTransferGetGroupKeysTest {

   public BackupOwnerStateTransferGetGroupKeysTest() {
      super(TestCacheFactory.BACKUP_OWNER);
   }
}
