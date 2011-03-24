package org.infinispan.transaction.xa;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.synchronization.SyncLocalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareDldGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareLocalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import java.util.Random;

/**
 * Factory for transaction related sate.
 *
 * @author Mircea.Markus@jboss.com
 */
public class TransactionFactory {

   private static Log log = LogFactory.getLog(TransactionFactory.class);

   private TxFactoryEnum txFactoryEnum;

   private enum TxFactoryEnum {

      DLD_RECOVERY_XA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx) {
            return new RecoveryAwareLocalTransaction(tx, gtx);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
            return addToinCoss(new RecoveryAwareDldGlobalTransaction(addr, remote));
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new RecoveryAwareDldGlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
            return new RecoveryAwareRemoteTransaction(modifications, tx);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx) {
            return new RecoveryAwareRemoteTransaction(tx);
         }
      },

      DLD_NORECOVERY_XA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx) {
            return new LocalXaTransaction(tx, gtx);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
            return addToinCoss(new DldGlobalTransaction(addr, remote));
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new DldGlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
            return new RemoteTransaction(modifications, tx);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx) {
            return new RemoteTransaction(tx);
         }
      },

      DLD_NORECOVERY_NOXA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx) {
            return new SyncLocalTransaction(tx, gtx);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
            return addToinCoss(new DldGlobalTransaction(addr, remote));
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new DldGlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
            return new RemoteTransaction(modifications, tx);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx) {
            return new RemoteTransaction(tx);
         }
      },
      NODLD_RECOVERY_XA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx) {
            return new RecoveryAwareLocalTransaction(tx, gtx);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
            return new RecoveryAwareGlobalTransaction(addr, remote);
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new RecoveryAwareGlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
            return new RecoveryAwareRemoteTransaction(modifications, tx);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx) {
            return new RecoveryAwareRemoteTransaction(tx);
         }
      },
      NODLD_NORECOVERY_XA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx) {
            return new LocalXaTransaction(tx, gtx);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
            return new GlobalTransaction(addr, remote);
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new GlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
            return new RemoteTransaction(modifications, tx);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx) {
            return new RemoteTransaction(tx);
         }
      },
      NODLD_NORECOVERY_NOXA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx) {
            return new SyncLocalTransaction(tx, gtx);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
            return new GlobalTransaction(addr, remote);
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new GlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
            return new RemoteTransaction(modifications, tx);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx) {
            return new RemoteTransaction(tx);
         }
      };


      public abstract LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx);
      public abstract GlobalTransaction newGlobalTransaction(Address addr, boolean remote);
      public abstract GlobalTransaction newGlobalTransaction();

      protected long generateRandomId() {
         return rnd.nextLong();
      }

      protected GlobalTransaction addToinCoss(DldGlobalTransaction dldGlobalTransaction) {
         dldGlobalTransaction.setCoinToss(generateRandomId());
         return dldGlobalTransaction;
      }

      /**
       * this class is internally synchronized, so it can be shared between instances
       */
      private final Random rnd = new Random();

      public abstract RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx);

      public abstract RemoteTransaction newRemoteTransaction(GlobalTransaction tx);
   }


   public GlobalTransaction newGlobalTransaction() {
      return txFactoryEnum.newGlobalTransaction();
   }

   public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
      return txFactoryEnum.newGlobalTransaction(addr, remote);
   }

   public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx) {
      return txFactoryEnum.newLocalTransaction(tx, gtx);
   }

   public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
      return txFactoryEnum.newRemoteTransaction(modifications, tx);
   }

   public RemoteTransaction newRemoteTransaction(GlobalTransaction tx) {
      return txFactoryEnum.newRemoteTransaction(tx);
   }


   public TransactionFactory() {
      init(false, false, true);
   }

   public TransactionFactory(boolean dldEnabled) {
      init(dldEnabled, false, true);
   }

   public TransactionFactory(boolean dldEnabled, boolean recoveryEnabled) {
      init(dldEnabled, recoveryEnabled, true);
   }


   @Inject
   public void init(Configuration configuration) {
      boolean dldEnabled = configuration.isEnableDeadlockDetection();
      boolean recoveryEnabled = configuration.isTransactionRecoveryEnabled();
      boolean xa = !configuration.isUseSynchronizationForTransactions();
      init(dldEnabled, recoveryEnabled, xa);
   }

   private void init(boolean dldEnabled, boolean recoveryEnabled, boolean xa) {
      if (dldEnabled && recoveryEnabled && xa) txFactoryEnum = TxFactoryEnum.DLD_RECOVERY_XA;
      if (dldEnabled && !recoveryEnabled && xa) txFactoryEnum = TxFactoryEnum.DLD_NORECOVERY_XA;
      if (dldEnabled && !recoveryEnabled && !xa) txFactoryEnum = TxFactoryEnum.DLD_NORECOVERY_NOXA;
      if (!dldEnabled && recoveryEnabled && xa) txFactoryEnum = TxFactoryEnum.NODLD_RECOVERY_XA;
      if (!dldEnabled && !recoveryEnabled && xa) txFactoryEnum = TxFactoryEnum.NODLD_NORECOVERY_XA;
      if (!dldEnabled && !recoveryEnabled && !xa) txFactoryEnum = TxFactoryEnum.NODLD_NORECOVERY_NOXA;

      if (log.isTraceEnabled()) log.trace("Setting factory enum to %s", txFactoryEnum);

      if (txFactoryEnum == null) {
         log.error("Unsupported combination (dldEnabled, recoveryEnabled, xa) = (%s, %s, %s)", dldEnabled, recoveryEnabled, xa);
         throw new IllegalStateException("Unsupported combination (dldEnabled, recoveryEnabled, xa) = (" + dldEnabled
                                               + ", " + recoveryEnabled + ", " + xa + ")");
      }
   }
}