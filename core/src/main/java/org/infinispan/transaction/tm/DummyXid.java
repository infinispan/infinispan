package org.infinispan.transaction.tm;

import javax.transaction.xa.Xid;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of Xid.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class DummyXid implements Xid {

   private static AtomicInteger txIdCounter = new AtomicInteger(0);
   private int id = txIdCounter.incrementAndGet();

   public int getFormatId() {
      return id;
   }

   public byte[] getGlobalTransactionId() {
      throw new IllegalStateException("TODO - please implement me!!!"); //todo implement!!!
   }

   public byte[] getBranchQualifier() {
      throw new IllegalStateException("TODO - please implement me!!!"); //todo implement!!!
   }

   @Override
   public String toString() {
      return "DummyXid{" +
            "id=" + id +
            '}';
   }
}
