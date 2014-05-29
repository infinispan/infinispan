package org.infinispan.distribution.groups;

import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface in the group's backup owner and in a non-transactional cache.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.BackupOwnerNonTxGetGroupKeysTest")
public class BackupOwnerNonTxGetGroupKeysTest extends BaseGetGroupKeysTest {

   public BackupOwnerNonTxGetGroupKeysTest() {
      super(false, TestCacheFactory.BACKUP_OWNER);
   }
}
