package org.infinispan.transaction.xa;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.recovery.RecoveryAwareDldGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareLocalTransaction;

import javax.transaction.Transaction;
import java.util.Random;

/**
 * Factory for transaction related sate.
 *
 * @author Mircea.Markus@jboss.com
 */
public class TransactionFactory {

   private boolean dldEnabled;
   private boolean recoveryEnabled;

   /**
    * this class is internally synchronized, so it can be shared between instances
    */
   private final Random rnd = new Random();

   private long generateRandomId() {
      return rnd.nextLong();
   }


   public TransactionFactory() {
   }

   public TransactionFactory(boolean dldEnabled) {
      this.dldEnabled = dldEnabled;
   }

   public TransactionFactory(boolean dldEnabled, boolean recoveryEnabled) {
      this(dldEnabled);
      this.recoveryEnabled = recoveryEnabled;
   }


   @Inject
   public void init(Configuration configuration) {
      dldEnabled = configuration.isEnableDeadlockDetection();
      recoveryEnabled = configuration.isTransactionRecoveryEnabled();
   }

   public GlobalTransaction instantiateGlobalTransaction() {
      if (dldEnabled) {
         if (recoveryEnabled) {
            return new RecoveryAwareDldGlobalTransaction();
         } else {
            return new DldGlobalTransaction();
         }
      } else {
         if (recoveryEnabled) {
            return new RecoveryAwareGlobalTransaction();
         } else {
            return new GlobalTransaction();
         }
      }
   }

   public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
      GlobalTransaction gtx;
      if (dldEnabled) {
         DldGlobalTransaction globalTransaction;
         if (recoveryEnabled) {
            globalTransaction = new RecoveryAwareDldGlobalTransaction(addr, remote);
         } else {
            globalTransaction = new DldGlobalTransaction(addr, remote);
         }
         globalTransaction.setCoinToss(generateRandomId());
         gtx = globalTransaction;
      } else {
         if (recoveryEnabled) {
            gtx = new RecoveryAwareGlobalTransaction(addr, remote);
         } else {
            gtx = new GlobalTransaction(addr, remote);
         }
      }
      return gtx;
   }

   public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx) {
      return recoveryEnabled ? new RecoveryAwareLocalTransaction(tx, gtx) : new LocalTransaction(tx, gtx);
   }

   public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
      return recoveryEnabled ? new RecoveryAwareRemoteTransaction(modifications, tx) : new RemoteTransaction(modifications, tx);
   }

   public RemoteTransaction newRemoteTransaction(GlobalTransaction tx) {
      return recoveryEnabled ? new RecoveryAwareRemoteTransaction(tx) : new RemoteTransaction(tx);
   }
}
