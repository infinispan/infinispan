package org.infinispan.transaction.tm;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.Xid;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.util.Util;

/**
 * Implementation of {@link Xid} used by {@link EmbeddedTransactionManager}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 9.0
 */
public final class EmbeddedXid implements Xid {

   //format can be anything except:
   //-1: means a null Xid
   // 0: means OSI CCR format
   //I would like ot use 0x4953504E (ISPN in hex) for the format, but keep it to 1 to be consistent with DummyXid.
   private static final int FORMAT = 1;
   private static final AtomicLong GLOBAL_ID_GENERATOR = new AtomicLong(1);
   private static final AtomicLong BRANCH_QUALIFIER_GENERATOR = new AtomicLong(1);
   private final int cachedHashcode;
   //note: AFAIK, the max size is 64 but it can be smaller. keep it until the DummyXid is removed.
   private byte[] globalTransactionId = new byte[64];
   private byte[] branchQualifier = new byte[64];

   public EmbeddedXid(UUID transactionManagerId) {
      cachedHashcode = initializeAndCalculateHash(transactionManagerId);
   }

   @Override
   public int getFormatId() {
      return FORMAT;
   }

   //the getter is not safe. we should clone it to prevent any modification
   @Override
   public byte[] getGlobalTransactionId() {
      return globalTransactionId;
   }

   @Override
   public byte[] getBranchQualifier() {
      return branchQualifier;
   }

   @Override
   public String toString() {
      return "EmbeddedXid{" +
            ", globalTransactionId = " + Util.printArray(globalTransactionId, false) +
            ", branchQualifier = " + Util.printArray(branchQualifier, false) +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) {
         return true;
      }
      if (o == null || !(o instanceof Xid)) {
         return false;
      }

      Xid other = (Xid) o;

      return other.getFormatId() == FORMAT &&
            Arrays.equals(branchQualifier, other.getBranchQualifier()) &&
            Arrays.equals(globalTransactionId, other.getGlobalTransactionId());
   }

   @Override
   public int hashCode() {
      return cachedHashcode;
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
}
