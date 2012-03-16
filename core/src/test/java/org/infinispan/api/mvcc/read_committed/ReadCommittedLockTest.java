/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.api.mvcc.read_committed;

import org.infinispan.Cache;
import org.infinispan.api.mvcc.LockTestBase;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

@Test(groups = "functional", testName = "api.mvcc.read_committed.ReadCommittedLockTest")
public class ReadCommittedLockTest extends LockTestBase {
   public ReadCommittedLockTest() {
      repeatableRead = false;
   }

   public void testVisibilityOfCommittedDataPut() throws Exception {
      Cache c = threadLocal.get().cache;
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
      Cache c = threadLocal.get().cache;
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

   public void testConcurrentWriters() throws Exception {
      super.testConcurrentWriters();
   }

}
