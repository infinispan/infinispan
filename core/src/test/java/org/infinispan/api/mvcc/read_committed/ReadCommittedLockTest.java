package org.infinispan.api.mvcc.read_committed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.Cache;
import org.infinispan.api.mvcc.LockTestBase;
import org.testng.annotations.Test;

import jakarta.transaction.Transaction;

@Test(groups = "functional", testName = "api.mvcc.read_committed.ReadCommittedLockTest")
public class ReadCommittedLockTest extends LockTestBase {
   public ReadCommittedLockTest() {
      repeatableRead = false;
   }

   public void testVisibilityOfCommittedDataPut() throws Exception {
      Cache<String, String> c = lockTestData.cache;
      c.put("k", "v");

      assertEquals("v", c.get("k"));

      // start a tx and read K
      lockTestData.tm.begin();
      assertEquals("v", c.get("k"));
      assertEquals("v", c.get("k"));
      Transaction reader = lockTestData.tm.suspend();

      lockTestData.tm.begin();
      c.put("k", "v2");
      Transaction writer = lockTestData.tm.suspend();

      lockTestData.tm.resume(reader);
      assertEquals("v", c.get("k"), "Should not read uncommitted data");
      reader = lockTestData.tm.suspend();

      lockTestData.tm.resume(writer);
      lockTestData.tm.commit();

      lockTestData.tm.resume(reader);
      assertEquals("v2", c.get("k"), "Should read committed data");
      lockTestData.tm.commit();
   }

   public void testVisibilityOfCommittedDataReplace() throws Exception {
      Cache<String, String> c = lockTestData.cache;
      c.put("k", "v");

      assertEquals("v", c.get("k"));

      // start a tx and read K
      lockTestData.tm.begin();
      assertEquals("v", c.get("k"));
      assertEquals("v", c.get("k"));
      Transaction reader = lockTestData.tm.suspend();

      lockTestData.tm.begin();
      c.replace("k", "v2");
      Transaction writer = lockTestData.tm.suspend();

      lockTestData.tm.resume(reader);
      assertEquals("v", c.get("k"), "Should not read uncommitted data");
      reader = lockTestData.tm.suspend();

      lockTestData.tm.resume(writer);
      lockTestData.tm.commit();

      lockTestData.tm.resume(reader);
      assertEquals("v2", c.get("k"), "Should read committed data");
      lockTestData.tm.commit();
   }

   @Override
   public void testConcurrentWriters() throws Exception {
      super.testConcurrentWriters();
   }

}
