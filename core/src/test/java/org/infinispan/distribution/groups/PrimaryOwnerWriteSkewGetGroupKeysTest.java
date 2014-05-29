package org.infinispan.distribution.groups;

import org.infinispan.transaction.TransactionProtocol;
import org.testng.annotations.Test;

/**
 * It tests the grouping advanced interface in the group's primary owner and in a transactional cache.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.groups.PrimaryOwnerWriteSkewGetGroupKeysTest")
public class PrimaryOwnerWriteSkewGetGroupKeysTest extends BaseWriteSkewGetGroupKeysTest {

   public PrimaryOwnerWriteSkewGetGroupKeysTest() {
      super(TestCacheFactory.PRIMARY_OWNER);
   }

   @Override
   protected TransactionProtocol getTransactionProtocol() {
      return TransactionProtocol.DEFAULT;
   }

}
