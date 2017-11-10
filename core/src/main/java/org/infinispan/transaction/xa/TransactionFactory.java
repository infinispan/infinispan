package org.infinispan.transaction.xa;

import java.util.Random;

import javax.transaction.Transaction;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.synchronization.SyncLocalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareDldGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareLocalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Factory for transaction related sate.
 *
 * @author Mircea.Markus@jboss.com
 */
public class TransactionFactory {

   private static final Log log = LogFactory.getLog(TransactionFactory.class);

   @Inject private Configuration configuration;
   @Inject @ComponentName(value = KnownComponentNames.TRANSACTION_VERSION_GENERATOR)
   private VersionGenerator clusterIdGenerator;
   @Inject private TimeService timeService;

   private TxFactoryEnum txFactoryEnum;
   private boolean isClustered;

   public enum TxFactoryEnum {
      @Deprecated
      DLD_RECOVERY_XA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction,
                                                     int topologyId,
                                                     long txCreationTime) {
            return new RecoveryAwareLocalTransaction(tx, gtx, implicitTransaction, topologyId, txCreationTime);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered) {
            RecoveryAwareDldGlobalTransaction dldGlobalTransaction = new RecoveryAwareDldGlobalTransaction(addr, remote);
            // TODO: Not ideal... but causes no issues so far. Could the internal id be an Object instead of a long?
            dldGlobalTransaction.setInternalId(((NumericVersion) clusterIdGenerator.generateNew()).getVersion());
            return addCoinToss(dldGlobalTransaction);
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new RecoveryAwareDldGlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx,
                                                       int topologyId, long txCreationTime) {
            return new RecoveryAwareRemoteTransaction(modifications, tx, topologyId, txCreationTime);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx, int topologyId,
                                                       long txCreationTime) {
            return new RecoveryAwareRemoteTransaction(tx, topologyId, txCreationTime);
         }
      },
      @Deprecated
      DLD_NORECOVERY_XA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction,
                                                     int topologyId,
                                                     long txCreationTime) {
            return new LocalXaTransaction(tx, gtx, implicitTransaction, topologyId, txCreationTime);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered) {
            return addCoinToss(new DldGlobalTransaction(addr, remote));
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new DldGlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx,
                                                       int topologyId,
                                                       long txCreationTime) {
            return new RemoteTransaction(modifications, tx, topologyId, txCreationTime);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx, int topologyId,
                                                       long txCreationTime) {
            return new RemoteTransaction(tx, topologyId, txCreationTime);
         }
      },
      @Deprecated
      DLD_NORECOVERY_NOXA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction,
                                                     int topologyId,
                                                     long txCreationTime) {
            return new SyncLocalTransaction(tx, gtx, implicitTransaction, topologyId, txCreationTime);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered) {
            return addCoinToss(new DldGlobalTransaction(addr, remote));
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new DldGlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx,
                                                       int topologyId,
                                                       long txCreationTime) {
            return new RemoteTransaction(modifications, tx, topologyId, txCreationTime);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx, int topologyId,
                                                       long txCreationTime) {
            return new RemoteTransaction(tx, topologyId, txCreationTime);
         }
      },
      NODLD_RECOVERY_XA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction,
                                                     int topologyId,
                                                     long txCreationTime) {
            return new RecoveryAwareLocalTransaction(tx, gtx, implicitTransaction, topologyId, txCreationTime);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered) {
            RecoveryAwareGlobalTransaction recoveryAwareGlobalTransaction = new RecoveryAwareGlobalTransaction(addr, remote);
            // TODO: Not ideal... but causes no issues so far. Could the internal id be an Object instead of a long?
            recoveryAwareGlobalTransaction.setInternalId(((NumericVersion) clusterIdGenerator.generateNew()).getVersion());
            return recoveryAwareGlobalTransaction;
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new RecoveryAwareGlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx,
                                                       int topologyId,
                                                       long txCreationTime) {
            return new RecoveryAwareRemoteTransaction(modifications, tx, topologyId, txCreationTime);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx, int topologyId,
                                                       long txCreationTime) {
            return new RecoveryAwareRemoteTransaction(tx, topologyId, txCreationTime);
         }
      },
      NODLD_NORECOVERY_XA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction,
                                                     int topologyId,
                                                     long txCreationTime) {
            return new LocalXaTransaction(tx, gtx, implicitTransaction, topologyId, txCreationTime);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered) {
            return new GlobalTransaction(addr, remote);
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new GlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx,
                                                       int topologyId,
                                                       long txCreationTime) {
            return new RemoteTransaction(modifications, tx, topologyId, txCreationTime);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx, int topologyId,
                                                       long txCreationTime) {
            return new RemoteTransaction(tx, topologyId, txCreationTime);
         }
      },
      NODLD_NORECOVERY_NOXA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction,
                                                     int topologyId,
                                                     long txCreationTime) {
            return new SyncLocalTransaction(tx, gtx, implicitTransaction, topologyId, txCreationTime);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered) {
            return new GlobalTransaction(addr, remote);
         }

         @Override
         public GlobalTransaction newGlobalTransaction() {
            return new GlobalTransaction();
         }

         @Override
         public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx,
                                                       int topologyId,
                                                       long txCreationTime) {
            return new RemoteTransaction(modifications, tx, topologyId, txCreationTime);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(GlobalTransaction tx, int topologyId,
                                                       long txCreationTime) {
            return new RemoteTransaction(tx, topologyId, txCreationTime);
         }
      };

      public abstract LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx,
                                                           boolean implicitTransaction, int topologyId,
                                                           long txCreationTime);

      public abstract GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered);
      public abstract GlobalTransaction newGlobalTransaction();

      protected long generateRandomId() {
         return rnd.nextLong();
      }

      protected GlobalTransaction addCoinToss(DldGlobalTransaction dldGlobalTransaction) {
         dldGlobalTransaction.setCoinToss(generateRandomId());
         return dldGlobalTransaction;
      }

      /**
       * this class is internally synchronized, so it can be shared between instances
       */
      private final Random rnd = new Random();

      public abstract RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx,
                                                             int topologyId, long txCreationTime);

      public abstract RemoteTransaction newRemoteTransaction(GlobalTransaction tx, int topologyId, long txCreationTime);
   }


   public GlobalTransaction newGlobalTransaction() {
      return txFactoryEnum.newGlobalTransaction();
   }

   public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
      return txFactoryEnum.newGlobalTransaction(addr, remote, this.clusterIdGenerator, isClustered);
   }

   public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction, int topologyId) {
      return txFactoryEnum.newLocalTransaction(tx, gtx, implicitTransaction, topologyId, timeService.time());
   }

   public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx, int topologyId) {
      return txFactoryEnum.newRemoteTransaction(modifications, tx, topologyId, timeService.time());
   }

   public RemoteTransaction newRemoteTransaction(GlobalTransaction tx, int topologyId) {
      return txFactoryEnum.newRemoteTransaction(tx, topologyId, timeService.time());
   }

   @Start
   public void start() {
      boolean xa = !configuration.transaction().useSynchronization();
      boolean recoveryEnabled = configuration.transaction().recovery().enabled();
      boolean batchingEnabled = configuration.invocationBatching().enabled();
      init(false, recoveryEnabled, xa, batchingEnabled);
      isClustered = configuration.clustering().cacheMode().isClustered();
   }

   public void init(boolean dldEnabled, boolean recoveryEnabled, boolean xa, boolean batchingEnabled) {
      if (batchingEnabled) {
         txFactoryEnum = TxFactoryEnum.NODLD_NORECOVERY_NOXA;
      } else {
         if (recoveryEnabled) {
            if (xa) {
               txFactoryEnum = TxFactoryEnum.NODLD_RECOVERY_XA;
            } else { //using synchronisation enlistment
               txFactoryEnum = TxFactoryEnum.NODLD_NORECOVERY_NOXA;
            }
         } else {
            if (xa) {
               txFactoryEnum = TxFactoryEnum.NODLD_NORECOVERY_XA;
            } else {
               txFactoryEnum = TxFactoryEnum.NODLD_NORECOVERY_NOXA;
            }
         }
      }
      log.tracef("Setting factory enum to %s", txFactoryEnum);
   }
}
