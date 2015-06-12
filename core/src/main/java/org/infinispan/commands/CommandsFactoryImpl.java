package org.infinispan.commands;

import org.infinispan.Cache;
import org.infinispan.commands.read.EntryRetrievalCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.context.InvocationContextFactory;
import org.infinispan.distribution.group.GroupManager;
import org.infinispan.iteration.impl.EntryRequestCommand;
import org.infinispan.iteration.impl.EntryResponseCommand;
import org.infinispan.iteration.impl.EntryRetriever;
import org.infinispan.metadata.Metadata;
import org.infinispan.atomic.Delta;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.read.ValuesCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderNonVersionedPrepareCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderRollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedPrepareCommand;
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
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.InternalEntryFactory;
import org.infinispan.context.Flag;
import org.infinispan.distexec.mapreduce.MapReduceManager;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.statetransfer.StateProvider;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.BackupSender;
import org.infinispan.xsite.SingleXSiteRpcCommand;
import org.infinispan.xsite.XSiteAdminCommand;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStateConsumer;
import org.infinispan.xsite.statetransfer.XSiteStateProvider;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferManager;

import javax.transaction.xa.Xid;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.infinispan.xsite.XSiteAdminCommand.*;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand.*;

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
   private CacheNotifier<Object, Object> notifier;
   private Cache<Object, Object> cache;
   private String cacheName;
   private boolean totalOrderProtocol;

   private InterceptorChain interceptorChain;
   private DistributionManager distributionManager;
   private InvocationContextFactory icf;
   private TransactionTable txTable;
   private Configuration configuration;
   private RecoveryManager recoveryManager;
   private StateProvider stateProvider;
   private StateConsumer stateConsumer;
   private LockManager lockManager;
   private InternalEntryFactory entryFactory;
   private MapReduceManager mapReduceManager;
   private StateTransferManager stateTransferManager;
   private BackupSender backupSender;
   private CancellationService cancellationService;
   private XSiteStateProvider xSiteStateProvider;
   private XSiteStateConsumer xSiteStateConsumer;
   private XSiteStateTransferManager xSiteStateTransferManager;
   private EntryRetriever entryRetriever;
   private GroupManager groupManager;

   private Map<Byte, ModuleCommandInitializer> moduleCommandInitializers;

   @Inject
   public void setupDependencies(DataContainer container, CacheNotifier<Object, Object> notifier, Cache<Object, Object> cache,
                                 InterceptorChain interceptorChain, DistributionManager distributionManager,
                                 InvocationContextFactory icf, TransactionTable txTable, Configuration configuration,
                                 @ComponentName(KnownComponentNames.MODULE_COMMAND_INITIALIZERS) Map<Byte, ModuleCommandInitializer> moduleCommandInitializers,
                                 RecoveryManager recoveryManager, StateProvider stateProvider, StateConsumer stateConsumer,
                                 LockManager lockManager, InternalEntryFactory entryFactory, MapReduceManager mapReduceManager, 
                                 StateTransferManager stm, BackupSender backupSender, CancellationService cancellationService,
                                 TimeService timeService, XSiteStateProvider xSiteStateProvider, XSiteStateConsumer xSiteStateConsumer,
                                 XSiteStateTransferManager xSiteStateTransferManager, EntryRetriever entryRetriever, GroupManager groupManager, PartitionHandlingManager partitionHandlingManager) {
      this.dataContainer = container;
      this.notifier = notifier;
      this.cache = cache;
      this.interceptorChain = interceptorChain;
      this.distributionManager = distributionManager;
      this.icf = icf;
      this.txTable = txTable;
      this.configuration = configuration;
      this.moduleCommandInitializers = moduleCommandInitializers;
      this.recoveryManager = recoveryManager;
      this.stateProvider = stateProvider;
      this.stateConsumer = stateConsumer;
      this.lockManager = lockManager;
      this.entryFactory = entryFactory;
      this.mapReduceManager = mapReduceManager;
      this.stateTransferManager = stm;
      this.backupSender = backupSender;
      this.cancellationService = cancellationService;
      this.xSiteStateConsumer = xSiteStateConsumer;
      this.xSiteStateProvider = xSiteStateProvider;
      this.xSiteStateTransferManager = xSiteStateTransferManager;
      this.entryRetriever = entryRetriever;
      this.groupManager = groupManager;
   }

   @Start(priority = 1)
   // needs to happen early on
   public void start() {
      cacheName = cache.getName();
      this.totalOrderProtocol = configuration.transaction().transactionProtocol().isTotalOrder();
   }

   @Override
   public PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, Metadata metadata, Set<Flag> flags) {
      return new PutKeyValueCommand(key, value, false, notifier, metadata, flags,
            configuration.dataContainer().valueEquivalence());
   }

   @Override
   public RemoveCommand buildRemoveCommand(Object key, Object value, Set<Flag> flags) {
      return new RemoveCommand(key, value, notifier, flags, configuration.dataContainer().valueEquivalence());
   }

   @Override
   public InvalidateCommand buildInvalidateCommand(Set<Flag> flags, Object... keys) {
      return new InvalidateCommand(notifier, flags, keys);
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(Set<Flag> flags, Collection<Object> keys) {
      return new InvalidateL1Command(dataContainer, configuration, distributionManager, notifier, flags, keys);
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(Address origin, Set<Flag> flags, Collection<Object> keys) {
      return new InvalidateL1Command(origin, dataContainer, configuration, distributionManager, notifier, flags, keys);
   }

   @Override
   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, Metadata metadata, Set<Flag> flags) {
      return new ReplaceCommand(key, oldValue, newValue, notifier, metadata, flags, configuration.dataContainer().valueEquivalence());
   }

   @Override
   public SizeCommand buildSizeCommand(Set<Flag> flags) {
      return new SizeCommand(cache, flags);
   }

   @Override
   public KeySetCommand buildKeySetCommand(Set<Flag> flags) {
      return new KeySetCommand(cache, flags);
   }

   @Override
   public ValuesCommand buildValuesCommand(Set<Flag> flags) {
      return new ValuesCommand(cache, flags);
   }

   @Override
   public EntrySetCommand buildEntrySetCommand(Set<Flag> flags) {
      return new EntrySetCommand(cache, flags);
   }

   @Override
   public EntryRetrievalCommand buildEntryRetrievalCommand(Set<Flag> flags, KeyValueFilter filter) {
      return new EntryRetrievalCommand(filter, entryRetriever, flags, cache);
   }

   @Override
   public GetKeyValueCommand buildGetKeyValueCommand(Object key, Set<Flag> flags) {
      return new GetKeyValueCommand(key, flags);
   }

   @Override
   public GetAllCommand buildGetAllCommand(Collection<?> keys,
         Set<Flag> flags, boolean returnEntries) {
      return new GetAllCommand(keys, flags, returnEntries, entryFactory);
   }

   @Override
   public PutMapCommand buildPutMapCommand(Map<?, ?> map, Metadata metadata, Set<Flag> flags) {
      return new PutMapCommand(map, notifier, metadata, flags);
   }

   @Override
   public ClearCommand buildClearCommand(Set<Flag> flags) {
      return new ClearCommand(notifier, dataContainer, flags);
   }

   @Override
   public EvictCommand buildEvictCommand(Object key, Set<Flag> flags) {
      return new EvictCommand(key, notifier, flags);
   }

   @Override
   public PrepareCommand buildPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit) {
      return totalOrderProtocol ? new TotalOrderNonVersionedPrepareCommand(cacheName, gtx, modifications) :
            new PrepareCommand(cacheName, gtx, modifications, onePhaseCommit);
   }

   @Override
   public VersionedPrepareCommand buildVersionedPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhase) {
      return totalOrderProtocol ? new TotalOrderVersionedPrepareCommand(cacheName, gtx, modifications, onePhase) :
            new VersionedPrepareCommand(cacheName, gtx, modifications, onePhase);
   }

   @Override
   public CommitCommand buildCommitCommand(GlobalTransaction gtx) {
      return totalOrderProtocol ? new TotalOrderCommitCommand(cacheName, gtx) :
            new CommitCommand(cacheName, gtx);
   }

   @Override
   public VersionedCommitCommand buildVersionedCommitCommand(GlobalTransaction gtx) {
      return totalOrderProtocol ? new TotalOrderVersionedCommitCommand(cacheName, gtx) :
            new VersionedCommitCommand(cacheName, gtx);
   }

   @Override
   public RollbackCommand buildRollbackCommand(GlobalTransaction gtx) {
      return totalOrderProtocol ? new TotalOrderRollbackCommand(cacheName, gtx) : new RollbackCommand(cacheName, gtx);
   }

   @Override
   public MultipleRpcCommand buildReplicateCommand(List<ReplicableCommand> toReplicate) {
      return new MultipleRpcCommand(toReplicate, cacheName);
   }

   @Override
   public SingleRpcCommand buildSingleRpcCommand(ReplicableCommand call) {
      return new SingleRpcCommand(cacheName, call);
   }

   @Override
   public ClusteredGetCommand buildClusteredGetCommand(Object key, Set<Flag> flags, boolean acquireRemoteLock, GlobalTransaction gtx) {
      return new ClusteredGetCommand(key, cacheName, flags, acquireRemoteLock, gtx,
            configuration.dataContainer().keyEquivalence());
   }

   /**
    * @param isRemote true if the command is deserialized and is executed remote.
    */
   @Override
   public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
      if (c == null) return;
      switch (c.getCommandId()) {
         case PutKeyValueCommand.COMMAND_ID:
            ((PutKeyValueCommand) c).init(notifier, configuration);
            break;
         case ReplaceCommand.COMMAND_ID:
            ((ReplaceCommand) c).init(notifier, configuration);
            break;
         case PutMapCommand.COMMAND_ID:
            ((PutMapCommand) c).init(notifier);
            break;
         case RemoveCommand.COMMAND_ID:
            ((RemoveCommand) c).init(notifier, configuration);
            break;
         case MultipleRpcCommand.COMMAND_ID:
            MultipleRpcCommand rc = (MultipleRpcCommand) c;
            rc.init(interceptorChain, icf);
            if (rc.getCommands() != null)
               for (ReplicableCommand nested : rc.getCommands()) {
                  initializeReplicableCommand(nested, false);
               }
            break;
         case SingleRpcCommand.COMMAND_ID:
            SingleRpcCommand src = (SingleRpcCommand) c;
            src.init(interceptorChain, icf);
            if (src.getCommand() != null)
               initializeReplicableCommand(src.getCommand(), false);

            break;
         case InvalidateCommand.COMMAND_ID:
            InvalidateCommand ic = (InvalidateCommand) c;
            ic.init(notifier, configuration);
            break;
         case InvalidateL1Command.COMMAND_ID:
            InvalidateL1Command ilc = (InvalidateL1Command) c;
            ilc.init(configuration, distributionManager, notifier, dataContainer);
            break;
         case PrepareCommand.COMMAND_ID:
         case VersionedPrepareCommand.COMMAND_ID:
         case TotalOrderNonVersionedPrepareCommand.COMMAND_ID:
         case TotalOrderVersionedPrepareCommand.COMMAND_ID:
            PrepareCommand pc = (PrepareCommand) c;
            pc.init(interceptorChain, icf, txTable);
            pc.initialize(notifier, recoveryManager);
            if (pc.getModifications() != null)
               for (ReplicableCommand nested : pc.getModifications())  {
                  initializeReplicableCommand(nested, false);
               }
            pc.markTransactionAsRemote(isRemote);
            if (configuration.deadlockDetection().enabled() && isRemote) {
               DldGlobalTransaction transaction = (DldGlobalTransaction) pc.getGlobalTransaction();
               transaction.setLocksHeldAtOrigin(pc.getAffectedKeys());
            }
            break;
         case CommitCommand.COMMAND_ID:
         case VersionedCommitCommand.COMMAND_ID:
         case TotalOrderCommitCommand.COMMAND_ID:
         case TotalOrderVersionedCommitCommand.COMMAND_ID:
            CommitCommand commitCommand = (CommitCommand) c;
            commitCommand.init(interceptorChain, icf, txTable);
            commitCommand.markTransactionAsRemote(isRemote);
            break;
         case RollbackCommand.COMMAND_ID:
         case TotalOrderRollbackCommand.COMMAND_ID:
            RollbackCommand rollbackCommand = (RollbackCommand) c;
            rollbackCommand.init(interceptorChain, icf, txTable);
            rollbackCommand.markTransactionAsRemote(isRemote);
            break;
         case ClearCommand.COMMAND_ID:
            ClearCommand cc = (ClearCommand) c;
            cc.init(notifier, dataContainer);
            break;
         case ClusteredGetCommand.COMMAND_ID:
            ClusteredGetCommand clusteredGetCommand = (ClusteredGetCommand) c;
            clusteredGetCommand.initialize(icf, this, entryFactory,
                  interceptorChain, distributionManager, txTable,
                  configuration.dataContainer().keyEquivalence());
            break;
         case LockControlCommand.COMMAND_ID:
            LockControlCommand lcc = (LockControlCommand) c;
            lcc.init(interceptorChain, icf, txTable);
            lcc.markTransactionAsRemote(isRemote);
            if (configuration.deadlockDetection().enabled() && isRemote) {
               DldGlobalTransaction gtx = (DldGlobalTransaction) lcc.getGlobalTransaction();
               RemoteTransaction transaction = txTable.getRemoteTransaction(gtx);
               if (transaction != null) {
                  if (!configuration.clustering().cacheMode().isDistributed()) {
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
         case StateRequestCommand.COMMAND_ID:
            ((StateRequestCommand) c).init(stateProvider);
            break;
         case StateResponseCommand.COMMAND_ID:
            ((StateResponseCommand) c).init(stateConsumer);
            break;
         case GetInDoubtTransactionsCommand.COMMAND_ID:
            GetInDoubtTransactionsCommand gptx = (GetInDoubtTransactionsCommand) c;
            gptx.init(recoveryManager);
            break;
         case TxCompletionNotificationCommand.COMMAND_ID:
            TxCompletionNotificationCommand ftx = (TxCompletionNotificationCommand) c;
            ftx.init(txTable, lockManager, recoveryManager, stateTransferManager);
            break;
         case MapCombineCommand.COMMAND_ID:
            MapCombineCommand mrc = (MapCombineCommand)c;
            mrc.init(mapReduceManager);
            break;
         case ReduceCommand.COMMAND_ID:
            ReduceCommand reduceCommand = (ReduceCommand)c;
            reduceCommand.init(mapReduceManager);
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
            break;
         case CreateCacheCommand.COMMAND_ID:
            CreateCacheCommand createCacheCommand = (CreateCacheCommand)c;
            createCacheCommand.init(cache.getCacheManager());
            break;
         case XSiteAdminCommand.COMMAND_ID:
            XSiteAdminCommand xSiteAdminCommand = (XSiteAdminCommand)c;
            xSiteAdminCommand.init(backupSender);
            break;
         case CancelCommand.COMMAND_ID:
            CancelCommand cancelCommand = (CancelCommand)c;
            cancelCommand.init(cancellationService);
            break;
         case XSiteStateTransferControlCommand.COMMAND_ID:
            XSiteStateTransferControlCommand xSiteStateTransferControlCommand = (XSiteStateTransferControlCommand) c;
            xSiteStateTransferControlCommand.initialize(xSiteStateProvider, xSiteStateConsumer, xSiteStateTransferManager);
            break;
         case XSiteStatePushCommand.COMMAND_ID:
            XSiteStatePushCommand xSiteStatePushCommand = (XSiteStatePushCommand) c;
            xSiteStatePushCommand.initialize(xSiteStateConsumer);
            break;
         case EntryRequestCommand.COMMAND_ID:
            EntryRequestCommand entryRequestCommand = (EntryRequestCommand) c;
            entryRequestCommand.init(entryRetriever);
            break;
         case EntryResponseCommand.COMMAND_ID:
            EntryResponseCommand entryResponseCommand = (EntryResponseCommand) c;
            entryResponseCommand.init(entryRetriever);
            break;
         case GetKeysInGroupCommand.COMMAND_ID:
            GetKeysInGroupCommand getKeysInGroupCommand = (GetKeysInGroupCommand) c;
            getKeysInGroupCommand.setGroupManager(groupManager);
            break;
         case ClusteredGetAllCommand.COMMAND_ID:
            ClusteredGetAllCommand clusteredGetAllCommand = (ClusteredGetAllCommand) c;
            clusteredGetAllCommand.init(icf, this, entryFactory, interceptorChain, txTable,
                  configuration.dataContainer().keyEquivalence());
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

   @Override
   public LockControlCommand buildLockControlCommand(Collection<?> keys, Set<Flag> flags, GlobalTransaction gtx) {
      return new LockControlCommand(keys, cacheName, flags, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Object key, Set<Flag> flags, GlobalTransaction gtx) {
      return new LockControlCommand(key, cacheName, flags, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection<?> keys, Set<Flag> flags) {
      return new LockControlCommand(keys,  cacheName, flags, null);
   }

   @Override
   public StateRequestCommand buildStateRequestCommand(StateRequestCommand.Type subtype, Address sender, int viewId, Set<Integer> segments) {
      return new StateRequestCommand(cacheName, subtype, sender, viewId, segments);
   }

   @Override
   public StateResponseCommand buildStateResponseCommand(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
      return new StateResponseCommand(cacheName, sender, topologyId, stateChunks);
   }

   @Override
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
      return new DistributedExecuteCommand<T>(cacheName, keys, callable);
   }

   @Override
   public <KIn, VIn, KOut, VOut> MapCombineCommand<KIn, VIn, KOut, VOut> buildMapCombineCommand(
            String taskId, Mapper<KIn, VIn, KOut, VOut> m, Reducer<KOut, VOut> r,
            Collection<KIn> keys) {
      return new MapCombineCommand<KIn, VIn, KOut, VOut>(taskId, m, r, cacheName, keys);
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

   @Override
   public CreateCacheCommand buildCreateCacheCommand(String cacheNameToCreate, String cacheConfigurationName) {
      return new CreateCacheCommand(cacheName, cacheNameToCreate, cacheConfigurationName);
   }

   @Override
   public CreateCacheCommand buildCreateCacheCommand(String cacheNameToCreate, String cacheConfigurationName, boolean start, int size) {
      return new CreateCacheCommand(cacheName, cacheNameToCreate, cacheConfigurationName, start, size);
   }

   @Override
   public <KOut, VOut> ReduceCommand<KOut, VOut> buildReduceCommand(String taskId,
            String destintationCache, Reducer<KOut, VOut> r, Collection<KOut> keys) {
      return new ReduceCommand<KOut, VOut>(taskId, r, destintationCache, keys);
   }

   @Override
   public CancelCommand buildCancelCommandCommand(UUID commandUUID) {
      return new CancelCommand(cacheName, commandUUID);
   }

   @Override
   public XSiteStateTransferControlCommand buildXSiteStateTransferControlCommand(StateTransferControl control,
                                                                                 String siteName) {
      return new XSiteStateTransferControlCommand(cacheName, control, siteName);
   }

   @Override
   public XSiteAdminCommand buildXSiteAdminCommand(String siteName, AdminOperation op, Integer afterFailures,
                                                   Long minTimeToWait) {
      return new XSiteAdminCommand(cacheName, siteName, op, afterFailures, minTimeToWait);
   }

   @Override
   public XSiteStatePushCommand buildXSiteStatePushCommand(XSiteState[] chunk, long timeoutMillis) {
      return new XSiteStatePushCommand(cacheName, chunk, timeoutMillis);
   }

   @Override
   public SingleXSiteRpcCommand buildSingleXSiteRpcCommand(VisitableCommand command) {
      return new SingleXSiteRpcCommand(cacheName, command);
   }

   @Override
   public <K, V, C> EntryRequestCommand<K, V, C> buildEntryRequestCommand(UUID identifier, Set<Integer> segments,
                                                                    Set<K> keysToFilter,
                                                                    KeyValueFilter<? super K, ? super V> filter,
                                                                    Converter<? super K, ? super V, C> converter,
                                                                    Set<Flag> flags) {
      return new EntryRequestCommand<K, V, C>(cacheName, identifier, cache.getCacheManager().getAddress(), segments,
                                              keysToFilter, filter, converter, flags);
   }

   @Override
   public <K, C> EntryResponseCommand<K, C> buildEntryResponseCommand(UUID identifier, Set<Integer> completedSegments,
                                                                Set<Integer> inDoubtSegments,
                                                                Collection<CacheEntry<K, C>> values, CacheException e) {
      return new EntryResponseCommand<>(cache.getCacheManager().getAddress(), cacheName, identifier, completedSegments,
                                      inDoubtSegments, values, e);
   }

   @Override
   public GetKeysInGroupCommand buildGetKeysInGroupCommand(Set<Flag> flags, String groupName) {
      return new GetKeysInGroupCommand(flags, groupName).setGroupManager(groupManager);
   }

   @Override
   public GetCacheEntryCommand buildGetCacheEntryCommand(Object key, Set<Flag> explicitFlags) {
      return new GetCacheEntryCommand(key, explicitFlags, entryFactory);
   }

   @Override
   public ClusteredGetAllCommand buildClusteredGetAllCommand(List<?> keys, Set<Flag> flags, GlobalTransaction gtx) {
      return new ClusteredGetAllCommand(cacheName, keys, flags, gtx, configuration.dataContainer().keyEquivalence());
   }

}
