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

import java.util.Arrays;
import java.util.UUID;

import javax.transaction.xa.Xid;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.util.Util;

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
      initialize(globalTransactionId);
      initialize(branchQualifier);
   }

   private void initialize(byte[] field) {
      UUID uuid = UUID.randomUUID();
      long lsb = uuid.getLeastSignificantBits();
      long msb = uuid.getMostSignificantBits();
      Arrays.fill(field, (byte) 0);
      UnsignedNumeric.writeUnsignedLong(field, 0, lsb);
      UnsignedNumeric.writeUnsignedLong(field, 10, msb);
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
