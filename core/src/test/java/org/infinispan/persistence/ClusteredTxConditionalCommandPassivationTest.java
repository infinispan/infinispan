package org.infinispan.persistence;

import org.testng.annotations.Test;

/**
 * Tests if the conditional commands correctly fetch the value from cache loader even with the skip cache load/store
 * flags.
 * <p/>
 * The configuration used is a tx distributed cache with passivation.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "persistence.ClusteredTxConditionalCommandPassivationTest")
public class ClusteredTxConditionalCommandPassivationTest extends ClusteredConditionalCommandTest {

   public ClusteredTxConditionalCommandPassivationTest() {
      super(true, true);
   }

   @Override
   protected <K, V> void assertLoadAfterOperation(CacheHelper<K, V> cacheHelper, ConditionalOperation operation, Ownership ownership, boolean skipLoad) {
      if (operation == ConditionalOperation.PUT_IF_ABSENT && !skipLoad) {
         //if the put if absent does not skip load, the transaction originator will load the entry from the cache store
         //and it will fail. This way, the transaction will not have any modifications.
         switch (ownership) {
            case PRIMARY_OWNER:
               assertLoad(cacheHelper, 1, 0, 0);
               break;
            case BACKUP_OWNER:
               assertLoad(cacheHelper, 0, 1, 0);
               break;
         }
      } else {
         //if the test is performed in the non_owner, it will fetch the data. sometimes, one of the nodes delays the
         // get and the test can fail randomly. So the assert is not performed in that case.
         if (ownership != Ownership.NON_OWNER) {
            assertLoad(cacheHelper, skipLoad ? 0 : 1, skipLoad ? 0 : 1, 0);
         }
      }
   }

}
