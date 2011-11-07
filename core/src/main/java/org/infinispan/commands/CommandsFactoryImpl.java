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
package org.infinispan.commands;

import org.infinispan.Cache;
import org.infinispan.atomic.Delta;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.MapReduceCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.Xid;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 4.0
 */
public class CommandsFactoryImpl implements CommandsFactory {

   private static final Log log = LogFactory.getLog(CommandsFactoryImpl.class);
   private static final boolean trace = log.isTraceEnabled();


   private DataContainer dataContainer;
   private CacheNotifier notifier;
   private Cache cache;
   private String cacheName;

   // some stateless commands can be reused so that they aren't constructed again all the time.
   SizeCommand cachedSizeCommand;
   KeySetCommand cachedKeySetCommand;
   ValuesCommand cachedValuesCommand;
   EntrySetCommand cachedEntrySetCommand;
   private InterceptorChain interceptorChain;
   private DistributionManager distributionManager;
   private InvocationContextContainer icc;
   private TransactionTable txTable;
   private Configuration configuration;
   private RecoveryManager recoveryManager;
   private StateTransferManager stateTransferManager;
   private LockManager lockManager;

   private Map<Byte, ModuleCommandInitializer> moduleCommandInitializers;

   @Inject
   public void setupDependencies(DataContainer container, CacheNotifier notifier, Cache cache,
                                 InterceptorChain interceptorChain, DistributionManager distributionManager,
                                 InvocationContextContainer icc, TransactionTable txTable, Configuration configuration,
                                 @ComponentName(KnownComponentNames.MODULE_COMMAND_INITIALIZERS) Map<Byte, ModuleCommandInitializer> moduleCommandInitializers,
                                 RecoveryManager recoveryManager, StateTransferManager stateTransferManager, LockManager lockManager) {
      this.dataContainer = container;
      this.notifier = notifier;
      this.cache = cache;
      this.interceptorChain = interceptorChain;
      this.distributionManager = distributionManager;
      this.icc = icc;
      this.txTable = txTable;
      this.configuration = configuration;
      this.moduleCommandInitializers = moduleCommandInitializers;
      this.recoveryManager = recoveryManager;
      this.stateTransferManager = stateTransferManager;
      this.lockManager = lockManager;
   }

   @Start(priority = 1)
   // needs to happen early on
   public void start() {
      cacheName = cache.getName();
   }

   public PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, long lifespanMillis, long maxIdleTimeMillis, Set<Flag> flags) {
      return new PutKeyValueCommand(key, value, false, notifier, lifespanMillis, maxIdleTimeMillis, flags);
   }

   public RemoveCommand buildRemoveCommand(Object key, Object value, Set<Flag> flags) {
      return new RemoveCommand(key, value, notifier, flags);
   }

   public InvalidateCommand buildInvalidateCommand(Object... keys) {
      return new InvalidateCommand(notifier, keys);
   }

   public InvalidateCommand buildInvalidateFromL1Command(boolean forRehash, Object... keys) {
      return new InvalidateL1Command(forRehash, dataContainer, configuration, distributionManager, notifier, keys);
   }

   public InvalidateCommand buildInvalidateFromL1Command(boolean forRehash, Collection<Object> keys) {
      return new InvalidateL1Command(forRehash, dataContainer, configuration, distributionManager, notifier, keys);
   }

   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, long lifespan, long maxIdleTimeMillis, Set<Flag> flags) {
      return new ReplaceCommand(key, oldValue, newValue, lifespan, maxIdleTimeMillis, flags);
   }

   public SizeCommand buildSizeCommand() {
      if (cachedSizeCommand == null) {
         cachedSizeCommand = new SizeCommand(dataContainer);
      }
      return cachedSizeCommand;
   }

   public KeySetCommand buildKeySetCommand() {
      if (cachedKeySetCommand == null) {
         cachedKeySetCommand = new KeySetCommand(dataContainer);
      }
      return cachedKeySetCommand;
   }

   public ValuesCommand buildValuesCommand() {
      if (cachedValuesCommand == null) {
         cachedValuesCommand = new ValuesCommand(dataContainer);
      }
      return cachedValuesCommand;
   }

   public EntrySetCommand buildEntrySetCommand() {
      if (cachedEntrySetCommand == null) {
         cachedEntrySetCommand = new EntrySetCommand(dataContainer);
      }
      return cachedEntrySetCommand;
   }

   public GetKeyValueCommand buildGetKeyValueCommand(Object key, Set<Flag> flags) {
      return new GetKeyValueCommand(key, notifier, flags);
   }

   public PutMapCommand buildPutMapCommand(Map map, long lifespan, long maxIdleTimeMillis, Set<Flag> flags) {
      return new PutMapCommand(map, notifier, lifespan, maxIdleTimeMillis, flags);
   }

   public ClearCommand buildClearCommand(Set<Flag> flags) {
      return new ClearCommand(notifier, flags);
   }

   public EvictCommand buildEvictCommand(Object key) {
      return new EvictCommand(key, notifier);
   }

   public PrepareCommand buildPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit) {
      return new PrepareCommand(cacheName, gtx, modifications, onePhaseCommit);
   }

   public CommitCommand buildCommitCommand(GlobalTransaction gtx) {
      return new CommitCommand(cacheName, gtx);
   }

   public RollbackCommand buildRollbackCommand(GlobalTransaction gtx) {
      return new RollbackCommand(cacheName, gtx);
   }

   public MultipleRpcCommand buildReplicateCommand(List<ReplicableCommand> toReplicate) {
      return new MultipleRpcCommand(toReplicate, cacheName);
   }

   public SingleRpcCommand buildSingleRpcCommand(ReplicableCommand call) {
      return new SingleRpcCommand(cacheName, call);
   }

   public ClusteredGetCommand buildClusteredGetCommand(Object key, Set<Flag> flags, boolean acquireRemoteLock, GlobalTransaction gtx) {
      return new ClusteredGetCommand(key, cacheName, flags, acquireRemoteLock, gtx);
   }

   /**
    * @param isRemote true if the command is deserialized and is executed remote.
    */
   public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
      if (c == null) return;
      switch (c.getCommandId()) {
         case PutKeyValueCommand.COMMAND_ID:
            ((PutKeyValueCommand) c).init(notifier);
            break;
         case PutMapCommand.COMMAND_ID:
            ((PutMapCommand) c).init(notifier);
            break;
         case RemoveCommand.COMMAND_ID:
            ((RemoveCommand) c).init(notifier);
            break;
         case MultipleRpcCommand.COMMAND_ID:
            MultipleRpcCommand rc = (MultipleRpcCommand) c;
            rc.init(interceptorChain, icc);
            if (rc.getCommands() != null)
               for (ReplicableCommand nested : rc.getCommands()) {
                  initializeReplicableCommand(nested, false);
               }
            break;
         case SingleRpcCommand.COMMAND_ID:
            SingleRpcCommand src = (SingleRpcCommand) c;
            src.init(interceptorChain, icc);
            if (src.getCommand() != null)
               initializeReplicableCommand(src.getCommand(), false);

            break;
         case InvalidateCommand.COMMAND_ID:
            InvalidateCommand ic = (InvalidateCommand) c;
            ic.init(notifier);
            break;
         case InvalidateL1Command.COMMAND_ID:
            InvalidateL1Command ilc = (InvalidateL1Command) c;
            ilc.init(configuration, distributionManager, notifier, dataContainer);
            break;
         case PrepareCommand.COMMAND_ID:
            PrepareCommand pc = (PrepareCommand) c;
            pc.init(interceptorChain, icc, txTable);
            pc.initialize(notifier, recoveryManager);
            if (pc.getModifications() != null)
               for (ReplicableCommand nested : pc.getModifications())  {
                  initializeReplicableCommand(nested, false);
               }
            pc.markTransactionAsRemote(isRemote);
            if (configuration.isEnableDeadlockDetection() && isRemote) {
               DldGlobalTransaction transaction = (DldGlobalTransaction) pc.getGlobalTransaction();
               transaction.setLocksHeldAtOrigin(pc.getAffectedKeys());
            }
            break;
         case CommitCommand.COMMAND_ID:
            CommitCommand commitCommand = (CommitCommand) c;
            commitCommand.init(interceptorChain, icc, txTable);
            commitCommand.markTransactionAsRemote(isRemote);
            break;
         case RollbackCommand.COMMAND_ID:
            RollbackCommand rollbackCommand = (RollbackCommand) c;
            rollbackCommand.init(interceptorChain, icc, txTable);
            rollbackCommand.markTransactionAsRemote(isRemote);
            break;
         case ClearCommand.COMMAND_ID:
            ClearCommand cc = (ClearCommand) c;
            cc.init(notifier);
            break;
         case ClusteredGetCommand.COMMAND_ID:
            ClusteredGetCommand clusteredGetCommand = (ClusteredGetCommand) c;
            clusteredGetCommand.initialize(icc, this, interceptorChain, distributionManager, txTable);
            break;
         case LockControlCommand.COMMAND_ID:
            LockControlCommand lcc = (LockControlCommand) c;
            lcc.init(interceptorChain, icc, txTable);
            lcc.markTransactionAsRemote(isRemote);
            if (configuration.isEnableDeadlockDetection() && isRemote) {
               DldGlobalTransaction gtx = (DldGlobalTransaction) lcc.getGlobalTransaction();
               RemoteTransaction transaction = txTable.getRemoteTransaction(gtx);
               if (transaction != null) {
                  if (!configuration.getCacheMode().isDistributed()) {
                     Set<Object> keys = txTable.getLockedKeysForRemoteTransaction(gtx);
                     GlobalTransaction gtx2 = transaction.getGlobalTransaction();
                     ((DldGlobalTransaction) gtx2).setLocksHeldAtOrigin(keys);
                     gtx.setLocksHeldAtOrigin(keys);
                  } else {
                     GlobalTransaction gtx2 = transaction.getGlobalTransaction();
                     ((DldGlobalTransaction) gtx2).setLocksHeldAtOrigin(gtx.getLocksHeldAtOrigin());
                  }
               }
            }
            break;
         case StateTransferControlCommand.COMMAND_ID:
            StateTransferControlCommand rcc = (StateTransferControlCommand) c;
            rcc.init(stateTransferManager, configuration, dataContainer, this);
            break;
         case GetInDoubtTransactionsCommand.COMMAND_ID:
            GetInDoubtTransactionsCommand gptx = (GetInDoubtTransactionsCommand) c;
            gptx.init(recoveryManager);
            break;
         case TxCompletionNotificationCommand.COMMAND_ID:
            TxCompletionNotificationCommand ftx = (TxCompletionNotificationCommand) c;
            ftx.init(txTable, lockManager, recoveryManager);
            break;
         case MapReduceCommand.COMMAND_ID:
            MapReduceCommand mrc = (MapReduceCommand)c;
            mrc.init(this, interceptorChain, icc, distributionManager,cache.getAdvancedCache().getRpcManager().getAddress());
            break;
         case DistributedExecuteCommand.COMMAND_ID:
            DistributedExecuteCommand dec = (DistributedExecuteCommand)c;
            dec.init(cache);
            break;
         case GetInDoubtTxInfoCommand.COMMAND_ID:
            GetInDoubtTxInfoCommand gidTxInfoCommand = (GetInDoubtTxInfoCommand)c;
            gidTxInfoCommand.init(recoveryManager);
            break;
         case CompleteTransactionCommand.COMMAND_ID:
            CompleteTransactionCommand ccc = (CompleteTransactionCommand)c;
            ccc.init(recoveryManager);
            break;
         case ApplyDeltaCommand.COMMAND_ID:
            ApplyDeltaCommand deltaCommand = (ApplyDeltaCommand) c;
            break;            
         default:
            ModuleCommandInitializer mci = moduleCommandInitializers.get(c.getCommandId());
            if (mci != null) {
               mci.initializeReplicableCommand(c, isRemote);
            } else {
               if (trace) log.tracef("Nothing to initialize for command: %s", c);
            }
      }
   }

   public LockControlCommand buildLockControlCommand(Collection keys, boolean implicit, Set<Flag> flags, GlobalTransaction gtx) {
      return new LockControlCommand(keys, cacheName, flags, gtx);
   }

   public LockControlCommand buildLockControlCommand(Object key, boolean implicit, Set<Flag> flags, GlobalTransaction gtx) {
      return new LockControlCommand(key, cacheName, flags, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection keys, Set<Flag> flags) {
      return new LockControlCommand(keys,  cacheName, flags, null);
   }

   public StateTransferControlCommand buildStateTransferCommand(StateTransferControlCommand.Type type, Address sender,
                                                                int viewId) {
      return new StateTransferControlCommand(cacheName, type, sender, viewId);
   }

   public StateTransferControlCommand buildStateTransferCommand(StateTransferControlCommand.Type type, Address sender,
                                                         int viewId, Collection<InternalCacheEntry> state) {
      return new StateTransferControlCommand(cacheName, type, sender, viewId, state);
   }

   public String getCacheName() {
      return cacheName;
   }

   @Override
   public GetInDoubtTransactionsCommand buildGetInDoubtTransactionsCommand() {
      return new GetInDoubtTransactionsCommand(cacheName);
   }

   @Override
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(Xid xid, GlobalTransaction globalTransaction) {
      return new TxCompletionNotificationCommand(xid, globalTransaction, cacheName);
   }

   @Override
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(long internalId) {
      return new TxCompletionNotificationCommand(internalId, cacheName);
   }

   @Override
   public <T> DistributedExecuteCommand<T> buildDistributedExecuteCommand(Callable<T> callable, Address sender, Collection keys) {
      return new DistributedExecuteCommand(keys, callable);
   }

   @Override
   public MapReduceCommand buildMapReduceCommand(Mapper m, Reducer r, Address sender, Collection keys) {
      return new MapReduceCommand(m, r, cacheName, keys);
   }

   @Override
   public GetInDoubtTxInfoCommand buildGetInDoubtTxInfoCommand() {
      return new GetInDoubtTxInfoCommand(cacheName);
   }

   @Override
   public CompleteTransactionCommand buildCompleteTransactionCommand(Xid xid, boolean commit) {
      return new CompleteTransactionCommand(cacheName, xid, commit);
   }

   @Override
   public ApplyDeltaCommand buildApplyDeltaCommand(Object deltaAwareValueKey, Delta delta, Collection keys) {
      return new ApplyDeltaCommand(deltaAwareValueKey, delta, keys);
   }
}
