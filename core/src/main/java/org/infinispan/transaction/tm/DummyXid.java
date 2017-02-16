package org.infinispan.transaction.tm;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.Xid;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.util.Util;

/**
 * Implementation of Xid.
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public final class DummyXid implements Xid {

   private static final AtomicLong GLOBAL_ID_GENERATOR = new AtomicLong(1);
   private static final AtomicLong BRANCH_QUALIFIER_GENERATOR = new AtomicLong(1);

   private byte[] globalTransactionId = new byte[64];
   private byte[] branchQualifier = new byte[64];
   private final int cachedHashcode;

   @Override
   public int getFormatId() {
      return 1;
   }

   public DummyXid(UUID transactionManagerId) {
      cachedHashcode = initializeAndCalculateHash(transactionManagerId);
   }

   @Override
   public byte[] getGlobalTransactionId() {
      return globalTransactionId;
   }

   @Override
   public byte[] getBranchQualifier() {
      return branchQualifier;
   }

   private int initializeAndCalculateHash(UUID transactionManagerId) {
      int hc1 = initialize(transactionManagerId, GLOBAL_ID_GENERATOR, globalTransactionId);
      return 37 * hc1 + initialize(transactionManagerId, BRANCH_QUALIFIER_GENERATOR, branchQualifier);
   }

   private int initialize(UUID transactionManagerId, AtomicLong generator, byte[] field) {
      long lsb = transactionManagerId.getLeastSignificantBits();
      long msb = transactionManagerId.getMostSignificantBits();
      long id = generator.getAndIncrement();
      Arrays.fill(field, (byte) 0);
      UnsignedNumeric.writeUnsignedLong(field, 0, lsb);
      UnsignedNumeric.writeUnsignedLong(field, 10, msb);
      UnsignedNumeric.writeUnsignedLong(field, 20, id);
      int hash = (int) (lsb ^ lsb >>> 32);
      hash = 37 * hash + (int) (msb ^ msb >>> 32);
      hash = 37 * hash + (int) (id ^ id >>> 32);
      return hash;
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

      if (other.getFormatId() != 1) return false;
      if (!Arrays.equals(branchQualifier, other.getBranchQualifier())) return false;
      if (!Arrays.equals(globalTransactionId, other.getGlobalTransactionId())) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return cachedHashcode;
   }
}
