package org.infinispan.functional.distribution.rehash;

import org.infinispan.AdvancedCache;
import org.infinispan.distribution.rehash.NonTxJoinerBecomingBackupOwnerTest;
import org.infinispan.functional.decorators.FunctionalAdvancedCache;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.op.TestFunctionalWriteOperation;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "distribution.rehash.FunctionalNonTxJoinerBecomingBackupOwnerTest")
@CleanupAfterMethod
public class FunctionalNonTxJoinerBecomingBackupOwnerTest extends NonTxJoinerBecomingBackupOwnerTest {

   @Override
   protected <A, B> AdvancedCache<A, B> advancedCache(int i) {
      AdvancedCache<A, B> cache = super.advancedCache(i);
      return FunctionalAdvancedCache.create(cache);
   }

   @Override
   public void testBackupOwnerJoiningDuringPut() throws Exception {
      doTest(TestFunctionalWriteOperation.PUT_CREATE_FUNCTIONAL);
   }

   @Override
   public void testBackupOwnerJoiningDuringPutIfAbsent() throws Exception {
      doTest(TestFunctionalWriteOperation.PUT_IF_ABSENT_FUNCTIONAL);
   }

   @Override
   public void testBackupOwnerJoiningDuringReplace() throws Exception {
      doTest(TestFunctionalWriteOperation.REPLACE_FUNCTIONAL);
   }

   @Override
   public void testBackupOwnerJoiningDuringReplaceWithPreviousValue() throws Exception {
      doTest(TestFunctionalWriteOperation.REPLACE_EXACT_FUNCTIONAL);
   }

   @Override
   public void testBackupOwnerJoiningDuringRemove() throws Exception {
      doTest(TestFunctionalWriteOperation.REMOVE_FUNCTIONAL);
   }

   @Override
   public void testBackupOwnerJoiningDuringRemoveWithPreviousValue() throws Exception {
      doTest(TestFunctionalWriteOperation.REMOVE_EXACT_FUNCTIONAL);
   }

}
