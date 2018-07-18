package org.infinispan.transaction.xa;

import java.io.IOException;
import java.io.ObjectInput;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This class is used when deadlock detection is enabled.
 *
 * @author Mircea.Markus@jboss.com
 * @deprecated Since 9.0, no longer used.
 */
@Deprecated
public class DldGlobalTransaction extends GlobalTransaction {

   private static final Log log = LogFactory.getLog(DldGlobalTransaction.class);

   private static final boolean trace = log.isTraceEnabled();

   private volatile long coinToss;

   private transient volatile Collection<Object> lockIntention = Collections.emptySet();

   private volatile Collection<?> remoteLockIntention = Collections.emptySet();

   protected volatile Collection<?> locksAtOrigin = Collections.emptySet();

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
      return super.toString() + ":dld:" + coinToss;
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

   public void setRemoteLockIntention(Collection<?> remoteLockIntention) {
      Objects.requireNonNull(lockIntention, "Remote lock intention must be non-null.");
      if (trace) {
         log.tracef("Setting the remote lock intention: %s", remoteLockIntention);
      }
      this.remoteLockIntention = remoteLockIntention;
   }

   public Collection<?> getRemoteLockIntention() {
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

   public void setLocksHeldAtOrigin(Collection<?> locksAtOrigin) {
      Objects.requireNonNull(locksAtOrigin, "Locks at origin must be non-null.");
      if (trace) log.tracef("Setting locks at origin for (%s) to %s", this, locksAtOrigin);
      this.locksAtOrigin = locksAtOrigin;
   }

   public Collection<?> getLocksHeldAtOrigin() {
      return this.locksAtOrigin;
   }

   @Deprecated
   public static class Externalizer extends GlobalTransaction.AbstractGlobalTxExternalizer<DldGlobalTransaction> {

      @Override
      protected DldGlobalTransaction createGlobalTransaction() {
         return new DldGlobalTransaction();
      }

      @Override
      public void writeObject(UserObjectOutput output, DldGlobalTransaction ddGt) throws IOException {
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
            ddGt.setLocksHeldAtOrigin(Collections.emptySet());
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
