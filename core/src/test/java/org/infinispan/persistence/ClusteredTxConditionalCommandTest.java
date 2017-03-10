package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.distribution.Ownership;
import org.testng.annotations.Test;

/**
 * Tests if the conditional commands correctly fetch the value from cache loader even with the skip cache load/store
 * flags.
 * <p/>
 * The configuration used is a tx distributed cache without passivation.
 *
 * @author Pedro Ruivo
 * @since 7.0
 */
@Test(groups = "functional", testName = "persistence.ClusteredTxConditionalCommandTest")
public class ClusteredTxConditionalCommandTest extends ClusteredConditionalCommandTest {

   // TX optimistic but without WSC!
   public ClusteredTxConditionalCommandTest() {
      super(true, false);
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
               // Replace_if does not load anything because it is unsuccessful on originator, as the retrieved
               // remote entry is null due to the skip load flag.
               assertLoad(cacheHelper, 0, 0, 0);
            } else {
               // The entry is loaded into DC upon the initial value retrieval (ClusteredGetCommand).
               // It gets loaded all the time on primary, but if the response to the retrieval does not
               // come soon enough, staggered logic sends second retrieval to backup owner, so it's possible
               // that both owners load once.
               assertEquals("primary owner load", 1, cacheHelper.loads(Ownership.PRIMARY));
               long backupLoads = cacheHelper.loads(Ownership.BACKUP);
               assertTrue("backup owner load: " + backupLoads, backupLoads <= 1);
               assertEquals("non owner load", 0, cacheHelper.loads(Ownership.NON_OWNER));
            }
            break;
      }
   }
}
