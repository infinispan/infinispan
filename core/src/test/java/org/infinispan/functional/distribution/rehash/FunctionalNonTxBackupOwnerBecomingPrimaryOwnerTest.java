package org.infinispan.functional.distribution.rehash;

import org.infinispan.distribution.rehash.NonTxBackupOwnerBecomingPrimaryOwnerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.op.TestFunctionalWriteOperation;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.rehash.FunctionalNonTxBackupOwnerBecomingPrimaryOwnerTest")
@CleanupAfterMethod
public class FunctionalNonTxBackupOwnerBecomingPrimaryOwnerTest extends NonTxBackupOwnerBecomingPrimaryOwnerTest {

   // TODO: Add more tests, e.g. read-write key operation

   public void testPrimaryOwnerChangingDuringReplaceBasedOnMeta() throws Exception {
      doTest(TestFunctionalWriteOperation.REPLACE_META_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringPut() throws Exception {
      doTest(TestFunctionalWriteOperation.PUT_CREATE_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringPutOverwrite() throws Exception {
      doTest(TestFunctionalWriteOperation.PUT_OVERWRITE_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringPutIfAbsent() throws Exception {
      doTest(TestFunctionalWriteOperation.PUT_IF_ABSENT_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringReplace() throws Exception {
      doTest(TestFunctionalWriteOperation.REPLACE_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringRemove() throws Exception {
      doTest(TestFunctionalWriteOperation.REMOVE_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringReplaceExact() throws Exception {
      doTest(TestFunctionalWriteOperation.REPLACE_EXACT_FUNCTIONAL);
   }

   @Override
   public void testPrimaryOwnerChangingDuringRemoveExact() throws Exception {
      doTest(TestFunctionalWriteOperation.REMOVE_EXACT_FUNCTIONAL);
   }

}
