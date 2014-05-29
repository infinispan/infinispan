package org.infinispan.distribution.groups;

import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface in the group's primary owner and in a non-transactional cache.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.PrimaryOwnerNonTxGetGroupKeysTest")
public class PrimaryOwnerNonTxGetGroupKeysTest extends BaseGetGroupKeysTest {

   public PrimaryOwnerNonTxGetGroupKeysTest() {
      super(false, TestCacheFactory.PRIMARY_OWNER);
   }

}
