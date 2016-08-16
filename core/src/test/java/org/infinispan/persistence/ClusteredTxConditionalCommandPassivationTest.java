package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

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
      switch (ownership) {
         case PRIMARY_OWNER:
            assertLoad(cacheHelper, skipLoad ? 0 : 1, 0, 0);
            break;
         case BACKUP_OWNER:
            assertLoad(cacheHelper, 0, skipLoad ? 0 : 1, 0);
            break;
         case NON_OWNER:
            if (!skipLoad) {
               assertTrue("any owner load", cacheHelper.loads(Ownership.PRIMARY_OWNER) +
                     cacheHelper.loads(Ownership.BACKUP_OWNER) >= 1);
               assertEquals("non owner load", 0, cacheHelper.loads(Ownership.NON_OWNER));
            } else {
               assertLoad(cacheHelper, 0, 0, 0);
            }
            break;
      }
   }

}
