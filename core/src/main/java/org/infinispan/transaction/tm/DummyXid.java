package org.infinispan.transaction.tm;

import org.infinispan.util.Util;

import javax.transaction.xa.Xid;
import java.util.Arrays;
import java.util.Random;

/**
 * Implementation of Xid.
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class DummyXid implements Xid {

   private byte[] globalTransactionId = new byte[64];
   private byte[] branchQualifier = new byte[64];

   public int getFormatId() {
      return 1;
   }

   public DummyXid() {
      initialize();
   }

   public byte[] getGlobalTransactionId() {
      return globalTransactionId;
   }

   public byte[] getBranchQualifier() {
      return branchQualifier;
   }

   private void initialize() {
      Random rnd = new Random();
      rnd.nextBytes(globalTransactionId);
      rnd.nextBytes(branchQualifier);
   }

   @Override
   public String toString() {
      return "DummyXid{" +
            ", globalTransactionId = " + Util.printArray(globalTransactionId, false) +
            ", branchQualifier = " + Util.printArray(branchQualifier, false) +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || !(o instanceof Xid)) return false;

      Xid other = (Xid) o;

      if (((Xid) o).getFormatId() != getFormatId()) return false;
      if (!Arrays.equals(branchQualifier, other.getBranchQualifier())) return false;
      if (!Arrays.equals(globalTransactionId, other.getGlobalTransactionId())) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = globalTransactionId != null ? Arrays.hashCode(globalTransactionId) : 0;
      result = 31 * result + (branchQualifier != null ? Arrays.hashCode(branchQualifier) : 0);
      return result;
   }
}
