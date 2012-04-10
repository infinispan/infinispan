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
package org.infinispan.transaction.tm;

import org.infinispan.io.UnsignedNumeric;
import org.infinispan.util.Util;

import javax.transaction.xa.Xid;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

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
