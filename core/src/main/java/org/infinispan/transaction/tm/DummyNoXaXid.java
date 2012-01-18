package org.infinispan.transaction.tm;

import javax.transaction.xa.Xid;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Xid to be used whe no XAResource enlistment takes place. This is more efficient both creation and memory wise than
 * {@link DummyXid}.
 *
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.1
 */
public class DummyNoXaXid implements Xid {

   private static final AtomicInteger txIdCounter = new AtomicInteger(0);
   private final int id = txIdCounter.incrementAndGet();

   public int getFormatId() {
      return id;
   }

   public byte[] getGlobalTransactionId() {
      throw new UnsupportedOperationException();
   }

   public byte[] getBranchQualifier() {
      throw new UnsupportedOperationException();
   }

   @Override
   public String toString() {
      return "DummyXid{" +
            "id=" + id +
            '}';
   }

   /**
    * Implementing an efficient hashCode is critical for performance:
    */
   @Override
   public final int hashCode() {
      return id;
   }

   @Override
   public final boolean equals(Object obj) {
      return this == obj;
   }

}
