/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.synchronization.SyncLocalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareDldGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareGlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareLocalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.util.ClusterIdGenerator;
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

   private static final Log log = LogFactory.getLog(TransactionFactory.class);

   private TxFactoryEnum txFactoryEnum;

   private EmbeddedCacheManager cm;
   private Configuration configuration;
   private ClusterIdGenerator clusterIdGenerator;
   private boolean isClustered;
   private RpcManager rpcManager;

   public enum TxFactoryEnum {

      DLD_RECOVERY_XA {
         @Override
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction) {
            return new RecoveryAwareLocalTransaction(tx, gtx, implicitTransaction);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, ClusterIdGenerator clusterIdGenerator, boolean clustered) {
            RecoveryAwareDldGlobalTransaction dldGlobalTransaction = new RecoveryAwareDldGlobalTransaction(addr, remote);
            dldGlobalTransaction.setInternalId(clusterIdGenerator.newVersion(clustered));
            return addCoinToss(dldGlobalTransaction);
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
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction) {
            return new LocalXaTransaction(tx, gtx, implicitTransaction);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, ClusterIdGenerator clusterIdGenerator, boolean clustered) {
            return addCoinToss(new DldGlobalTransaction(addr, remote));
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
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction) {
            return new SyncLocalTransaction(tx, gtx, implicitTransaction);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, ClusterIdGenerator clusterIdGenerator, boolean clustered) {
            return addCoinToss(new DldGlobalTransaction(addr, remote));
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
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction) {
            return new RecoveryAwareLocalTransaction(tx, gtx, implicitTransaction);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, ClusterIdGenerator clusterIdGenerator, boolean clustered) {
            RecoveryAwareGlobalTransaction recoveryAwareGlobalTransaction = new RecoveryAwareGlobalTransaction(addr, remote);
            recoveryAwareGlobalTransaction.setInternalId(clusterIdGenerator.newVersion(clustered));
            return recoveryAwareGlobalTransaction;
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
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction) {
            return new LocalXaTransaction(tx, gtx, implicitTransaction);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, ClusterIdGenerator clusterIdGenerator, boolean clustered) {
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
         public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction) {
            return new SyncLocalTransaction(tx, gtx, implicitTransaction);
         }

         @Override
         public GlobalTransaction newGlobalTransaction(Address addr, boolean remote, ClusterIdGenerator clusterIdGenerator, boolean clustered) {
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


      public abstract LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction);
      public abstract GlobalTransaction newGlobalTransaction(Address addr, boolean remote, ClusterIdGenerator clusterIdGenerator, boolean clustered);
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

      public abstract RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx);

      public abstract RemoteTransaction newRemoteTransaction(GlobalTransaction tx);
   }


   public GlobalTransaction newGlobalTransaction() {
      return txFactoryEnum.newGlobalTransaction();
   }

   public GlobalTransaction newGlobalTransaction(Address addr, boolean remote) {
      return txFactoryEnum.newGlobalTransaction(addr, remote, this.clusterIdGenerator, isClustered);
   }

   public LocalTransaction newLocalTransaction(Transaction tx, GlobalTransaction gtx, boolean implicitTransaction) {
      return txFactoryEnum.newLocalTransaction(tx, gtx, implicitTransaction);
   }

   public RemoteTransaction newRemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
      return txFactoryEnum.newRemoteTransaction(modifications, tx);
   }

   public RemoteTransaction newRemoteTransaction(GlobalTransaction tx) {
      return txFactoryEnum.newRemoteTransaction(tx);
   }

   @Inject
   public void init(Configuration configuration, EmbeddedCacheManager cm, RpcManager rpcManager) {
      this.cm = cm;
      this.configuration = configuration;
      this.rpcManager = rpcManager;
   }

   @Start
   public void start() {
      boolean dldEnabled = configuration.isDeadlockDetectionEnabled();
      boolean xa = !configuration.isUseSynchronizationForTransactions();
      boolean recoveryEnabled = configuration.isTransactionRecoveryEnabled();
      init(dldEnabled, recoveryEnabled, xa);
      isClustered = configuration.getCacheMode().isClustered();
      if (recoveryEnabled) {
         Transport transport = rpcManager != null ? rpcManager.getTransport() : null;
         clusterIdGenerator = new ClusterIdGenerator(cm, transport);
      }
   }

   public void init(boolean dldEnabled, boolean recoveryEnabled, boolean xa) {
      if (dldEnabled && recoveryEnabled && xa) {
         txFactoryEnum = TxFactoryEnum.DLD_RECOVERY_XA;
      } else if (dldEnabled && !recoveryEnabled && xa) {
         txFactoryEnum = TxFactoryEnum.DLD_NORECOVERY_XA;
      } else if (dldEnabled && !recoveryEnabled && !xa) {
         txFactoryEnum = TxFactoryEnum.DLD_NORECOVERY_NOXA;
      } else  if (!dldEnabled && recoveryEnabled && xa) {
         txFactoryEnum = TxFactoryEnum.NODLD_RECOVERY_XA;
      } else if (!dldEnabled && !recoveryEnabled && xa) {
         txFactoryEnum = TxFactoryEnum.NODLD_NORECOVERY_XA;
      } else if (!dldEnabled && !recoveryEnabled && !xa) {
         txFactoryEnum = TxFactoryEnum.NODLD_NORECOVERY_NOXA;
      }
      if (log.isTraceEnabled()) log.tracef("Setting factory enum to %s", txFactoryEnum);

      if (txFactoryEnum == null) {
         log.unsupportedTransactionConfiguration(dldEnabled, recoveryEnabled, xa);
         throw new IllegalStateException(String.format(
               "Unsupported combination (dldEnabled, recoveryEnabled, xa) = (%s, %s, %s)",
               dldEnabled, recoveryEnabled, xa));
      }
   }
}