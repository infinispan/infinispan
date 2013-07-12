package org.infinispan.api.mvcc.read_committed;

import org.infinispan.Cache;
import org.infinispan.api.mvcc.LockTestBase;
import org.testng.annotations.Test;

import javax.transaction.Transaction;

@Test(groups = "functional", testName = "api.mvcc.read_committed.ReadCommittedLockTest")
public class ReadCommittedLockTest extends LockTestBase {
   public ReadCommittedLockTest() {
      repeatableRead = false;
   }

   public void testVisibilityOfCommittedDataPut() throws Exception {
      Cache<String, String> c = threadLocal.get().cache;
      c.put("k", "v");

      assert "v".equals(c.get("k"));

      // start a tx and read K
      threadLocal.get().tm.begin();
      assert "v".equals(c.get("k"));
      assert "v".equals(c.get("k"));
      Transaction reader = threadLocal.get().tm.suspend();

      threadLocal.get().tm.begin();
      c.put("k", "v2");
      Transaction writer = threadLocal.get().tm.suspend();

      threadLocal.get().tm.resume(reader);
      assert "v".equals(c.get("k")) : "Should not read uncommitted data";
      reader = threadLocal.get().tm.suspend();

      threadLocal.get().tm.resume(writer);
      threadLocal.get().tm.commit();

      threadLocal.get().tm.resume(reader);
      assert "v2".equals(c.get("k")) : "Should read committed data";
      threadLocal.get().tm.commit();
   }

   public void testVisibilityOfCommittedDataReplace() throws Exception {
      Cache<String, String> c = threadLocal.get().cache;
      c.put("k", "v");

      assert "v".equals(c.get("k"));

      // start a tx and read K
      threadLocal.get().tm.begin();
      assert "v".equals(c.get("k"));
      assert "v".equals(c.get("k"));
      Transaction reader = threadLocal.get().tm.suspend();

      threadLocal.get().tm.begin();
      c.replace("k", "v2");
      Transaction writer = threadLocal.get().tm.suspend();

      threadLocal.get().tm.resume(reader);
      assert "v".equals(c.get("k")) : "Should not read uncommitted data";
      reader = threadLocal.get().tm.suspend();

      threadLocal.get().tm.resume(writer);
      threadLocal.get().tm.commit();

      threadLocal.get().tm.resume(reader);
      assert "v2".equals(c.get("k")) : "Should read committed data";
      threadLocal.get().tm.commit();
   }

   @Override
   public void testConcurrentWriters() throws Exception {
      super.testConcurrentWriters();
   }

}
