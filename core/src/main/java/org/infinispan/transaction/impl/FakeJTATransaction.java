package org.infinispan.transaction.impl;

import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.tx.TransactionImpl;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.Util;

public class FakeJTATransaction extends TransactionImpl {
   // Make it different from embedded txs (1)
   static final int FORMAT_ID = 2;
   static AtomicLong id = new AtomicLong(0);

   public FakeJTATransaction() {
      byte[] bytes = new byte[8];
      Util.longToBytes(id.incrementAndGet(), bytes, 0);
      XidImpl xid = XidImpl.create(FORMAT_ID, bytes, bytes);
      setXid(xid);
   }
}
