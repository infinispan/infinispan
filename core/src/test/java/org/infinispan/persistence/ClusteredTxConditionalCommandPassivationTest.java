package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.distribution.Ownership;
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
         case PRIMARY:
            assertLoad(cacheHelper, skipLoad ? 0 : 1, operation.successful(skipLoad) && !skipLoad ? 1 : 0, 0);
            break;
         case BACKUP:
            assertLoad(cacheHelper, operation.successful(skipLoad) && !skipLoad ? 1 : 0, skipLoad ? 0 : 1, 0);
            break;
         case NON_OWNER:
            if (skipLoad) {
               assertLoad(cacheHelper, 0, 0, 0);
            } else {
               // The entry is loaded into DC upon the initial value retrieval (ClusteredGetCommand).
               // It is loaded on primary, but if the response to the retrieval does not
               // come soon enough, staggered logic sends second retrieval to backup owner, so it's possible
               // that both owners load once.
               // It is also possible that the primary does not get the request while backup handles the read
               // - then we won't see any load on primary owner.
               long primaryLoads = cacheHelper.loads(Ownership.PRIMARY);
               assertTrue("primary owner load: " + primaryLoads, primaryLoads <= 1);
               long backupLoads = cacheHelper.loads(Ownership.BACKUP);
               assertTrue("backup owner load: " + backupLoads, backupLoads <= 1);
               assertTrue("loads: primary=" + primaryLoads + ", backup=" + backupLoads, primaryLoads + backupLoads >= 1);
               assertEquals("non owner load", 0, cacheHelper.loads(Ownership.NON_OWNER));
            }
            break;
      }
   }

}
