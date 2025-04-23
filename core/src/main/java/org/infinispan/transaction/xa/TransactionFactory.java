package org.infinispan.transaction.xa;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import jakarta.transaction.Transaction;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.versioning.NumericVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.synchronization.SyncLocalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareLocalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Factory for transaction related state.
 *
 * @author Mircea.Markus@jboss.com
 */
@Scope(Scopes.NAMED_CACHE)
public class TransactionFactory {

   private static final Log log = LogFactory.getLog(TransactionFactory.class);

   @Inject Configuration configuration;
   @Inject @ComponentName(value = KnownComponentNames.TRANSACTION_VERSION_GENERATOR)
   VersionGenerator clusterIdGenerator;
   @Inject TimeService timeService;

   private TxFactoryEnum txFactoryEnum;
   private boolean isClustered;

   public enum TxFactoryEnum {
      NODLD_RECOVERY_XA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction,
                                                     int topologyId,
                                                     long txCreationTime) {
            return new RecoveryAwareLocalTransaction(tx, gtx, implicitTransaction, topologyId, txCreationTime);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered, boolean client) {
            GlobalTransaction recoveryAwareGlobalTransaction = new GlobalTransaction(addr, remote, client);
            // TODO: Not ideal... but causes no issues so far. Could the internal id be an Object instead of a long?
            recoveryAwareGlobalTransaction.setInternalId(((NumericVersion) clusterIdGenerator.generateNew()).getVersion());
            return recoveryAwareGlobalTransaction;
         }

         @Override
         public RemoteTransaction newRemoteTransaction(List<WriteCommand> modifications, GlobalTransaction tx, int topologyId, long txCreationTime) {
            return new RecoveryAwareRemoteTransaction(modifications, tx, topologyId, txCreationTime);
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
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered, boolean client) {
            return new GlobalTransaction(addr, remote, client);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(List<WriteCommand> modifications, GlobalTransaction tx, int topologyId, long txCreationTime) {
            return new RemoteTransaction(modifications, tx, topologyId, txCreationTime);
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
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered, boolean client) {
            return new GlobalTransaction(addr, remote, client);
         }

         @Override
         public RemoteTransaction newRemoteTransaction(List<WriteCommand> modifications, GlobalTransaction tx, int topologyId, long txCreationTime) {
            return new RemoteTransaction(modifications, tx, topologyId, txCreationTime);
         }
      };

      public abstract LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx,
                                                           boolean implicitTransaction, int topologyId,
                                                           long txCreationTime);

      @Deprecated(since = "16.0", forRemoval = true)
      public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered) {
         return newGlobalTransaction(addr, remote, clusterIdGenerator, clustered, false);
      }

      public abstract GlobalTransaction newGlobalTransaction(Address addr, boolean remote, VersionGenerator clusterIdGenerator, boolean clustered, boolean client);

      public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx,
                                                             int topologyId, long txCreationTime) {
         return newRemoteTransaction(Arrays.asList(modifications), tx, topologyId, txCreationTime);
      }

      public RemoteTransaction newRemoteTransaction(GlobalTransaction tx, int topologyId, long txCreationTime) {
         return newRemoteTransaction(Collections.emptyList(), tx, topologyId, txCreationTime);
      }

      public abstract RemoteTransaction newRemoteTransaction(List<WriteCommand> modifications, GlobalTransaction tx,
                                                             int topologyId, long txCreationTime);
   }

   public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
      return txFactoryEnum.newGlobalTransaction(addr, remote, this.clusterIdGenerator, isClustered);
   }

   public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, boolean client) {
      return txFactoryEnum.newGlobalTransaction(addr, remote, this.clusterIdGenerator, isClustered, client);
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

   public RemoteTransaction newRemoteTransaction(List<WriteCommand> modifications, GlobalTransaction gtx, int topologyId) {
      return txFactoryEnum.newRemoteTransaction(modifications, gtx, topologyId, timeService.time());
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
