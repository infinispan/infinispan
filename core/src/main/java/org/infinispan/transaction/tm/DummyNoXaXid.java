package org.infinispan.transaction.tm;

import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.Xid;

/**
 * Xid to be used when no XAResource enlistment takes place. This is more efficient both creation and memory wise than
 * {@link DummyXid}.
 *
 * @author Mircea Markus &lt;mircea.markus@jboss.com&gt; (C) 2011 Red Hat Inc.
 * @since 5.1
 * @deprecated it will be removed and {@link EmbeddedXid} would be used instead.
 */
@Deprecated
public final class DummyNoXaXid implements Xid {

   private static final AtomicInteger txIdCounter = new AtomicInteger(0);
   private final int id = txIdCounter.incrementAndGet();

   @Override
   public int getFormatId() {
      return id;
   }

   @Override
   public byte[] getGlobalTransactionId() {
      throw new UnsupportedOperationException();
   }

   @Override
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
