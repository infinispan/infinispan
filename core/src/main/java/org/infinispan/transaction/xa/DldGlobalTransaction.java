/*
 * JBoss, Home of Professional Open Source
 * Copyright 2010 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.transaction.xa;

import static java.util.Collections.emptySet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Set;

import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.util.Util;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This class is used when deadlock detection is enabled.
 *
 * @author Mircea.Markus@jboss.com
 */
public class DldGlobalTransaction extends GlobalTransaction {

   private static final Log log = LogFactory.getLog(DldGlobalTransaction.class);

   private static final boolean trace = log.isTraceEnabled();

   protected volatile long coinToss;

   protected volatile boolean isMarkedForRollback;

   protected transient volatile Object localLockIntention;

   protected volatile Collection<Object> remoteLockIntention = emptySet();

   protected volatile Set<Object> locksAtOrigin = emptySet();

   public DldGlobalTransaction() {
   }

   public DldGlobalTransaction(Address addr, boolean remote) {
      super(addr, remote);
   }


   /**
    * Sets the random number that defines the coin toss. A coin toss is a random number that is used when a deadlock is
    * detected for deciding which transaction should commit and which should rollback.
    */
   public void setCoinToss(long coinToss) {
      this.coinToss = coinToss;
   }

   public long getCoinToss() {
      return coinToss;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof DldGlobalTransaction)) return false;
      if (!super.equals(o)) return false;

      DldGlobalTransaction that = (DldGlobalTransaction) o;

      if (coinToss != that.coinToss) return false;
      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (int) (coinToss ^ (coinToss >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "DldGlobalTransaction{" +
            "coinToss=" + coinToss +
            ", isMarkedForRollback=" + isMarkedForRollback +
            ", lockIntention=" + localLockIntention +
            ", affectedKeys=" + remoteLockIntention +
            ", locksAtOrigin=" + locksAtOrigin +
            "} " + super.toString();
   }

   /**
    * Returns the key this transaction intends to lock. 
    */
   public Object getLockIntention() {
      return localLockIntention;
   }

   public void setLockIntention(Object lockIntention) {
      if (trace) log.tracef("Setting local lock intention to %s", lockIntention);
      this.localLockIntention = lockIntention;
   }

   public boolean wouldLose(DldGlobalTransaction other) {
      return this.coinToss < other.coinToss;
   }

   public void setRemoteLockIntention(Collection<Object> remoteLockIntention) {
      if (trace) {
         log.tracef("Setting the remote lock intention: %s", remoteLockIntention);
      }
      this.remoteLockIntention = remoteLockIntention;
   }

   public Collection<Object> getRemoteLockIntention() {
      return remoteLockIntention;
   }

   public boolean hasLockAtOrigin(Collection<Object> remoteLockIntention) {
      if (log.isTraceEnabled())
         log.tracef("Our(%s) locks at origin are: %s. Others remote lock intention is: %s",
                    this, locksAtOrigin, remoteLockIntention);
      for (Object key : remoteLockIntention) {
         if (this.locksAtOrigin.contains(key)) {
            return true;
         }
      }
      return false;
   }

   public void setLocksHeldAtOrigin(Set<Object> locksAtOrigin) {
      if (trace) log.tracef("Setting locks at origin for (%s) to %s", this, locksAtOrigin);
      this.locksAtOrigin = locksAtOrigin;
   }

   public Set<Object> getLocksHeldAtOrigin() {
      return this.locksAtOrigin;
   }

   public static class Externalizer extends GlobalTransaction.AbstractGlobalTxExternalizer<DldGlobalTransaction> {

      @Override
      protected DldGlobalTransaction createGlobalTransaction() {
         return (DldGlobalTransaction) TransactionFactory.TxFactoryEnum.DLD_NORECOVERY_XA.newGlobalTransaction();
      }

      @Override
      public void writeObject(ObjectOutput output, DldGlobalTransaction ddGt) throws IOException {
         super.writeObject(output, ddGt);
         output.writeLong(ddGt.getCoinToss());
         if (ddGt.locksAtOrigin.isEmpty()) {
            output.writeObject(null);
         } else {
            output.writeObject(ddGt.locksAtOrigin);
         }
      }

      @Override
      @SuppressWarnings("unchecked")
      public DldGlobalTransaction readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         DldGlobalTransaction ddGt = super.readObject(input);
         ddGt.setCoinToss(input.readLong());
         Object locksAtOriginObj = input.readObject();
         if (locksAtOriginObj == null) {
            ddGt.setLocksHeldAtOrigin(emptySet());
         } else {
            ddGt.setLocksHeldAtOrigin((Set<Object>) locksAtOriginObj);
         }
         return ddGt;
      }

      @Override
      public Integer getId() {
         return Ids.DEADLOCK_DETECTING_GLOBAL_TRANSACTION;
      }

      @Override
      public Set<Class<? extends DldGlobalTransaction>> getTypeClasses() {
         return Util.<Class<? extends DldGlobalTransaction>>asSet(DldGlobalTransaction.class);
      }
   }
}
