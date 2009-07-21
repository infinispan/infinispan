package org.infinispan.transaction.xa;

import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

/**
 * This class is used when deadlock detection is enabled.
 *
 * @author Mircea.Markus@jboss.com
 */
@Marshallable(externalizer = DeadlockDetectingGlobalTransaction.Externalizer.class, id = Ids.DEADLOCK_DETECTING_GLOBAL_TRANSACTION)
public class DeadlockDetectingGlobalTransaction extends GlobalTransaction {

   private static Log log = LogFactory.getLog(DeadlockDetectingGlobalTransaction.class);

   public static final boolean trace = log.isTraceEnabled();

   private Set<Address> replicatingTo = Collections.EMPTY_SET;

   private volatile transient Thread processingThread;

   private volatile long coinToss;

   private volatile boolean isMarkedForRollback;

   private transient volatile Object lockInterntion;


   public DeadlockDetectingGlobalTransaction() {
   }

   DeadlockDetectingGlobalTransaction(Address addr, boolean remote) {
      super(addr, remote);
   }

   DeadlockDetectingGlobalTransaction(boolean remote) {
      super(null, remote);
   }

   /**
    * Is this global transaction replicating to the given address?
    */
   public boolean isReplicatingTo(Address address) {
      if (this.replicatingTo == null) {
         return true;
      } else {
         return this.replicatingTo.contains(address);
      }
   }

   /**
    * On a node, this will set the thread that handles replicatin on the given node.
    */
   public void setProcessingThread(Thread replicationThread) {
      if (trace) log.trace("Setting thread " +  Thread.currentThread() + "on tx ["  + this + "]");
      this.processingThread = replicationThread;
   }

   /**
    * Tries to interrupt the processing thread.
    */
   public synchronized void interruptProcessingThread() {
      if (isMarkedForRollback) {
         if (trace) log.trace("Not interrupting as tx is marked for rollback");
         return;
      }
      if (processingThread == null) {
         if(trace) log.trace("Processing thread is null, nothing to interrupt");
         return;
      }
      if (trace) {
         StackTraceElement[] stackTraceElements = processingThread.getStackTrace();
         StringBuilder builder = new StringBuilder();
         for (StackTraceElement stackTraceElement : stackTraceElements) {
            builder.append("            ").append(stackTraceElement).append('\n');
         }
         log.trace("About to interrupt thread: " + processingThread + ". Thread's stack trace is: \n" + builder.toString());
      }
      this.processingThread.interrupt();
   }

   /**
    * Sets the set og <b>Address</b> objects this node is replicating to.
    */
   public void setReplicatingTo(Set<Address> targets) {
      this.replicatingTo = targets;
   }

   /**
    * Based on the coin toss, determine whether this tx will continue working or this thread will be stopped.  
    */
   public boolean thisWillInterrupt(DeadlockDetectingGlobalTransaction globalTransaction) {
      return this.coinToss > globalTransaction.coinToss;
   }

   /**
    * Sets the reandom number that defines the coin toss.
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
      if (!(o instanceof DeadlockDetectingGlobalTransaction)) return false;
      if (!super.equals(o)) return false;

      DeadlockDetectingGlobalTransaction that = (DeadlockDetectingGlobalTransaction) o;

      if (coinToss != that.coinToss) return false;
      if (replicatingTo != null ? !replicatingTo.equals(that.replicatingTo) : that.replicatingTo != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (replicatingTo != null ? replicatingTo.hashCode() : 0);
      result = 31 * result + (int) (coinToss ^ (coinToss >>> 32));
      return result;
   }

   @Override
   public String toString() {
      return "DeadlockDetectingGlobalTransaction{" +
            "replicatingTo=" + replicatingTo +
            ", replicationThread=" + processingThread +
            ", coinToss=" + coinToss +
            "} " + super.toString();
   }

   /**
    * Once marked for rollback, the call to {@link #interruptProcessingThread()} will be ignored.
    */
   public synchronized boolean isMarkedForRollback() {
      return isMarkedForRollback;
   }

   public synchronized void setMarkedForRollback(boolean markedForRollback) {
      isMarkedForRollback = markedForRollback;
   }

   /**
    * Returns the key this transaction intends to lock. 
    */
   public Object getLockInterntion() {
      return lockInterntion;
   }

   /**
    * Sets the lock this transaction intends to lock.
    */
   public void setLockInterntion(Object lockInterntion) {
      this.lockInterntion = lockInterntion;
   }
   
   public static class Externalizer extends GlobalTransaction.Externalizer {
      public Externalizer() {
         gtxFactory = new GlobalTransactionFactory(true);
      }

      @Override
      public void writeObject(ObjectOutput output, Object subject) throws IOException {
         super.writeObject(output, subject);
         DeadlockDetectingGlobalTransaction ddGt = (DeadlockDetectingGlobalTransaction) subject;
         output.writeLong(ddGt.getCoinToss());
      }

      @Override
      public Object readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         DeadlockDetectingGlobalTransaction ddGt = (DeadlockDetectingGlobalTransaction) super.readObject(input);
         ddGt.setCoinToss(input.readLong());
         return ddGt;
      }
   }
}
