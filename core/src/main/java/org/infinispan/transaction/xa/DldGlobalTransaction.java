package org.infinispan.transaction.xa;

import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;

/**
 * This class is used when deadlock detection is enabled.
 *
 * @author Mircea.Markus@jboss.com
 */
public class DldGlobalTransaction extends GlobalTransaction {

   private static final Log log = LogFactory.getLog(DldGlobalTransaction.class);

   private static final boolean trace = log.isTraceEnabled();

   protected volatile long coinToss;

   protected transient volatile Collection<Object> lockIntention = InfinispanCollections.emptySet();

   protected volatile Collection<Object> remoteLockIntention = InfinispanCollections.emptySet();

   protected volatile Set<Object> locksAtOrigin = InfinispanCollections.emptySet();

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
            ", lockIntention=" + lockIntention +
            ", affectedKeys=" + remoteLockIntention +
            ", locksAtOrigin=" + locksAtOrigin +
            "} " + super.toString();
   }

   /**
    * Returns the key this transaction intends to lock. 
    */
   public Collection<Object> getLockIntention() {
      return lockIntention;
   }

   public void setLockIntention(Collection<Object> lockIntention) {
      Objects.requireNonNull(lockIntention, "Local lock Intention must be non-null.");
      if (trace) log.tracef("Setting local lock intention to %s", lockIntention);
      this.lockIntention = lockIntention;
   }

   public boolean wouldLose(DldGlobalTransaction other) {
      return this.coinToss < other.coinToss;
   }

   public void setRemoteLockIntention(Collection<Object> remoteLockIntention) {
      Objects.requireNonNull(lockIntention, "Remote lock intention must be non-null.");
      if (trace) {
         log.tracef("Setting the remote lock intention: %s", remoteLockIntention);
      }
      this.remoteLockIntention = remoteLockIntention;
   }

   public Collection<Object> getRemoteLockIntention() {
      return remoteLockIntention;
   }

   public boolean hasAnyLockAtOrigin(DldGlobalTransaction otherTx) {
      if (trace) {
         log.tracef("Our(%s) locks at origin are: %s. Others remote lock intention is: %s",
                    this, locksAtOrigin, otherTx.getRemoteLockIntention());
      }
      for (Object key : otherTx.getRemoteLockIntention()) {
         if (this.locksAtOrigin.contains(key)) {
            return true;
         }
      }
      return false;
   }

   public void setLocksHeldAtOrigin(Set<Object> locksAtOrigin) {
      Objects.requireNonNull(locksAtOrigin, "Locks at origin must be non-null.");
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
            ddGt.setLocksHeldAtOrigin(InfinispanCollections.emptySet());
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
