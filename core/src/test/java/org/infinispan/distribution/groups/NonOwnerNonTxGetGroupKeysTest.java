package org.infinispan.distribution.groups;

import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface in the group's non owner and in a non-transactional cache.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.NonOwnerNonTxGetGroupKeysTest")
public class NonOwnerNonTxGetGroupKeysTest extends BaseGetGroupKeysTest {

   public NonOwnerNonTxGetGroupKeysTest() {
      super(false, TestCacheFactory.NON_OWNER);
   }

}
