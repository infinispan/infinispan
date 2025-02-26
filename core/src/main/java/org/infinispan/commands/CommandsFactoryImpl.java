package org.infinispan.commands;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.Mutation;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.TxReadOnlyKeyCommand;
import org.infinispan.commands.functional.TxReadOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.irac.IracCleanupKeysCommand;
import org.infinispan.commands.irac.IracMetadataRequestCommand;
import org.infinispan.commands.irac.IracPrimaryPendingKeyCheckCommand;
import org.infinispan.commands.irac.IracRequestStateCommand;
import org.infinispan.commands.irac.IracStateResponseCommand;
import org.infinispan.commands.irac.IracTombstoneCleanupCommand;
import org.infinispan.commands.irac.IracTombstonePrimaryCheckCommand;
import org.infinispan.commands.irac.IracTombstoneRemoteSiteCheckCommand;
import org.infinispan.commands.irac.IracTombstoneStateResponseCommand;
import org.infinispan.commands.irac.IracUpdateVersionCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.CheckTransactionRpcCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.statetransfer.ConflictResolutionStartCommand;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferGetListenersCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commands.triangle.BackupNoopCommand;
import org.infinispan.commands.triangle.MultiEntriesFunctionalBackupWriteCommand;
import org.infinispan.commands.triangle.MultiKeyFunctionalBackupWriteCommand;
import org.infinispan.commands.triangle.PutMapBackupWriteCommand;
import org.infinispan.commands.triangle.SingleKeyBackupWriteCommand;
import org.infinispan.commands.triangle.SingleKeyFunctionalBackupWriteCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracTombstoneInfo;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.encoding.DataConversion;
import org.infinispan.expiration.impl.TouchCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.impl.ComponentRef;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.functional.EntryView.ReadEntryView;
import org.infinispan.functional.EntryView.ReadWriteEntryView;
import org.infinispan.functional.EntryView.WriteEntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.MultiClusterEventCommand;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.commands.batch.CancelPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.NextPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.reduction.ReductionPublisherRequestCommand;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.SingleXSiteRpcCommand;
import org.infinispan.xsite.commands.XSiteAmendOfflineStatusCommand;
import org.infinispan.xsite.commands.XSiteAutoTransferStatusCommand;
import org.infinispan.xsite.commands.XSiteSetStateTransferModeCommand;
import org.infinispan.xsite.commands.XSiteStateTransferCancelSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferClearStatusCommand;
import org.infinispan.xsite.commands.XSiteStateTransferFinishReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferFinishSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferRestartSendingCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartReceiveCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStartSendCommand;
import org.infinispan.xsite.commands.XSiteStateTransferStatusRequestCommand;
import org.infinispan.xsite.commands.remote.IracClearKeysRequest;
import org.infinispan.xsite.commands.remote.IracPutManyRequest;
import org.infinispan.xsite.commands.remote.IracTombstoneCheckRequest;
import org.infinispan.xsite.commands.remote.IracTouchKeyRequest;
import org.infinispan.xsite.commands.remote.XSiteStatePushRequest;
import org.infinispan.xsite.commands.remote.XSiteStateTransferControlRequest;
import org.infinispan.xsite.irac.IracManagerKeyInfo;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.reactivestreams.Publisher;

/**
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public class CommandsFactoryImpl implements CommandsFactory {
   private static final Log log = LogFactory.getLog(CommandsFactoryImpl.class);

   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject Configuration configuration;
   @Inject ComponentRef<Cache<Object, Object>> cache;
   @Inject ComponentRegistry componentRegistry;
   @Inject EmbeddedCacheManager cacheManager;
   @Inject @ComponentName(KnownComponentNames.INTERNAL_MARSHALLER)
   Marshaller marshaller;

   private ByteString cacheName;
   private boolean transactional;

   @Start
   // needs to happen early on
   public void start() {
      cacheName = ByteString.fromString(cache.wired().getName());
      this.transactional = configuration.transaction().transactionMode().isTransactional();
   }

   @Override
   public PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, int segment, Metadata metadata,
                                                     long flagsBitSet, boolean returnEntry) {
      boolean reallyTransactional = transactional && !EnumUtil.containsAny(flagsBitSet, FlagBitSets.PUT_FOR_EXTERNAL_READ);
      return new PutKeyValueCommand(key, value, false, returnEntry, metadata, segment, flagsBitSet, generateUUID(reallyTransactional));
   }

   @Override
   public RemoveCommand buildRemoveCommand(Object key, Object value, int segment, long flagsBitSet, boolean returnEntry) {
      return new RemoveCommand(key, value, returnEntry, segment, flagsBitSet, generateUUID(transactional));
   }

   @Override
   public InvalidateCommand buildInvalidateCommand(long flagsBitSet, Object... keys) {
      // StateConsumerImpl always uses non-tx invalidation
      return new InvalidateCommand(flagsBitSet, generateUUID(false), keys);
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(long flagsBitSet, Collection<Object> keys) {
      // StateConsumerImpl always uses non-tx invalidation
      return new InvalidateL1Command(flagsBitSet, keys, generateUUID(transactional));
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(Address origin, long flagsBitSet, Collection<Object> keys) {
      // L1 invalidation is always non-transactional
      return new InvalidateL1Command(origin, flagsBitSet, keys, generateUUID(false));
   }

   @Override
   public RemoveExpiredCommand buildRemoveExpiredCommand(Object key, Object value, int segment, Long lifespan,
         long flagsBitSet) {
      return new RemoveExpiredCommand(key, value, lifespan, false, segment, flagsBitSet,
            generateUUID(false));
   }

   @Override
   public RemoveExpiredCommand buildRemoveExpiredCommand(Object key, Object value, int segment, long flagsBitSet) {
      return new RemoveExpiredCommand(key, value, null, true, segment, flagsBitSet,
            generateUUID(false));
   }

   @Override
   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, int segment,
                                             Metadata metadata, long flagsBitSet, boolean returnEntry) {
      return new ReplaceCommand(key, oldValue, newValue, returnEntry, metadata, segment, flagsBitSet, generateUUID(transactional));
   }

   @Override
   public ComputeCommand buildComputeCommand(Object key, BiFunction mappingFunction, boolean computeIfPresent, int segment, Metadata metadata, long flagsBitSet) {
      return init(new ComputeCommand(key, mappingFunction, computeIfPresent, segment, flagsBitSet, generateUUID(transactional), metadata));
   }

   @Override
   public ComputeIfAbsentCommand buildComputeIfAbsentCommand(Object key, Function mappingFunction, int segment, Metadata metadata, long flagsBitSet) {
      return init(new ComputeIfAbsentCommand(key, mappingFunction, segment, flagsBitSet, generateUUID(transactional), metadata));
   }

   @Override
   public SizeCommand buildSizeCommand(IntSet segments, long flagsBitSet) {
      return new SizeCommand(cacheName, segments, flagsBitSet);
   }

   @Override
   public KeySetCommand buildKeySetCommand(long flagsBitSet) {
      return new KeySetCommand<>(flagsBitSet);
   }

   @Override
   public EntrySetCommand buildEntrySetCommand(long flagsBitSet) {
      return new EntrySetCommand<>(flagsBitSet);
   }

   @Override
   public GetKeyValueCommand buildGetKeyValueCommand(Object key, int segment, long flagsBitSet) {
      return new GetKeyValueCommand(key, segment, flagsBitSet);
   }

   @Override
   public GetAllCommand buildGetAllCommand(Collection<?> keys, long flagsBitSet, boolean returnEntries) {
      return new GetAllCommand(keys, flagsBitSet, returnEntries);
   }

   @Override
   public PutMapCommand buildPutMapCommand(Map<?, ?> map, Metadata metadata, long flagsBitSet) {
      return new PutMapCommand(map, metadata, flagsBitSet, generateUUID(transactional));
   }

   @Override
   public ClearCommand buildClearCommand(long flagsBitSet) {
      return new ClearCommand(flagsBitSet);
   }

   @Override
   public EvictCommand buildEvictCommand(Object key, int segment, long flagsBitSet) {
      return new EvictCommand(key, segment, flagsBitSet, generateUUID(transactional));
   }

   @Override
   public PrepareCommand buildPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit) {
      return new PrepareCommand(cacheName, gtx, modifications, onePhaseCommit);
   }

   @Override
   public VersionedPrepareCommand buildVersionedPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhase) {
      return new VersionedPrepareCommand(cacheName, gtx, modifications, onePhase);
   }

   @Override
   public CommitCommand buildCommitCommand(GlobalTransaction gtx) {
      return new CommitCommand(cacheName, gtx);
   }

   @Override
   public VersionedCommitCommand buildVersionedCommitCommand(GlobalTransaction gtx) {
      return new VersionedCommitCommand(cacheName, gtx);
   }

   @Override
   public RollbackCommand buildRollbackCommand(GlobalTransaction gtx) {
      return new RollbackCommand(cacheName, gtx);
   }

   @Override
   public SingleRpcCommand buildSingleRpcCommand(VisitableCommand call) {
      return new SingleRpcCommand(cacheName, call);
   }

   @Override
   public ClusteredGetCommand buildClusteredGetCommand(Object key, Integer segment, long flagsBitSet) {
      return new ClusteredGetCommand(key, cacheName, segment, flagsBitSet);
   }

   /**
    * @param isRemote true if the command is deserialized and is executed remote.
    */
   @Override
   public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
      if (c == null) return;

      if (c instanceof InitializableCommand)
         ((InitializableCommand) c).init(componentRegistry, isRemote);
   }

   @SuppressWarnings("unchecked")
   private <T> T init(VisitableCommand cmd) {
      cmd.init(componentRegistry);
      return (T) cmd;
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection<?> keys, long flagsBitSet, GlobalTransaction gtx) {
      return new LockControlCommand(keys, cacheName, flagsBitSet, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Object key, long flagsBitSet, GlobalTransaction gtx) {
      return new LockControlCommand(key, cacheName, flagsBitSet, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection<?> keys, long flagsBitSet) {
      return new LockControlCommand(keys, cacheName, flagsBitSet, null);
   }

   @Override
   public ConflictResolutionStartCommand buildConflictResolutionStartCommand(int topologyId, IntSet segments) {
      return new ConflictResolutionStartCommand(cacheName, topologyId, segments);
   }

   @Override
   public StateTransferCancelCommand buildStateTransferCancelCommand(int topologyId, IntSet segments) {
      return new StateTransferCancelCommand(cacheName, topologyId, segments);
   }

   @Override
   public StateTransferGetListenersCommand buildStateTransferGetListenersCommand(int topologyId) {
      return new StateTransferGetListenersCommand(cacheName, topologyId);
   }

   @Override
   public StateTransferGetTransactionsCommand buildStateTransferGetTransactionsCommand(int topologyId, IntSet segments) {
      return new StateTransferGetTransactionsCommand(cacheName, topologyId, segments);
   }

   @Override
   public StateTransferStartCommand buildStateTransferStartCommand(int topologyId, IntSet segments) {
      return new StateTransferStartCommand(cacheName, topologyId, segments);
   }

   @Override
   public StateResponseCommand buildStateResponseCommand(int topologyId, Collection<StateChunk> stateChunks, boolean applyState) {
      return new StateResponseCommand(cacheName, topologyId, stateChunks, applyState);
   }

   @Override
   public String getCacheName() {
      return cacheName.toString();
   }

   @Override
   public GetInDoubtTransactionsCommand buildGetInDoubtTransactionsCommand() {
      return new GetInDoubtTransactionsCommand(cacheName);
   }

   @Override
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(XidImpl xid, GlobalTransaction globalTransaction) {
      return new TxCompletionNotificationCommand(xid, globalTransaction, cacheName);
   }

   @Override
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(long internalId) {
      return new TxCompletionNotificationCommand(internalId, cacheName);
   }

   @Override
   public GetInDoubtTxInfoCommand buildGetInDoubtTxInfoCommand() {
      return new GetInDoubtTxInfoCommand(cacheName);
   }

   @Override
   public CompleteTransactionCommand buildCompleteTransactionCommand(XidImpl xid, boolean commit) {
      return new CompleteTransactionCommand(cacheName, xid, commit);
   }

   @Override
   public XSiteStateTransferCancelSendCommand buildXSiteStateTransferCancelSendCommand(String siteName) {
      return new XSiteStateTransferCancelSendCommand(cacheName, siteName);
   }

   @Override
   public XSiteStateTransferClearStatusCommand buildXSiteStateTransferClearStatusCommand() {
      return new XSiteStateTransferClearStatusCommand(cacheName);
   }

   @Override
   public XSiteStateTransferFinishReceiveCommand buildXSiteStateTransferFinishReceiveCommand(String siteName) {
      return new XSiteStateTransferFinishReceiveCommand(cacheName, siteName);
   }

   @Override
   public XSiteStateTransferFinishSendCommand buildXSiteStateTransferFinishSendCommand(String siteName, boolean statusOk) {
      return new XSiteStateTransferFinishSendCommand(cacheName, siteName, statusOk);
   }

   @Override
   public XSiteStateTransferRestartSendingCommand buildXSiteStateTransferRestartSendingCommand(String siteName, int topologyId) {
      return new XSiteStateTransferRestartSendingCommand(cacheName, siteName, topologyId);
   }

   @Override
   public XSiteStateTransferStartReceiveCommand buildXSiteStateTransferStartReceiveCommand(String siteName) {
      return new XSiteStateTransferStartReceiveCommand(cacheName, siteName);
   }

   @Override
   public XSiteStateTransferStartSendCommand buildXSiteStateTransferStartSendCommand(String siteName, int topologyId) {
      return new XSiteStateTransferStartSendCommand(cacheName, siteName, topologyId);
   }

   @Override
   public XSiteStateTransferStatusRequestCommand buildXSiteStateTransferStatusRequestCommand() {
      return new XSiteStateTransferStatusRequestCommand(cacheName);
   }

   @Override
   public XSiteStateTransferControlRequest buildXSiteStateTransferControlRequest(boolean startReceiving) {
      return new XSiteStateTransferControlRequest(cacheName, startReceiving);
   }

   @Override
   public XSiteAmendOfflineStatusCommand buildXSiteAmendOfflineStatusCommand(String siteName, Integer afterFailures, Long minTimeToWait) {
      return new XSiteAmendOfflineStatusCommand(cacheName, siteName, afterFailures, minTimeToWait);
   }

   @Override
   public XSiteStatePushCommand buildXSiteStatePushCommand(List<XSiteState> chunk) {
      return new XSiteStatePushCommand(cacheName, chunk);
   }

   @Override
   public SingleXSiteRpcCommand buildSingleXSiteRpcCommand(VisitableCommand command) {
      return new SingleXSiteRpcCommand(cacheName, command);
   }

   @Override
   public GetCacheEntryCommand buildGetCacheEntryCommand(Object key, int segment, long flagsBitSet) {
      return new GetCacheEntryCommand(key, segment, flagsBitSet);
   }

   @Override
   public ClusteredGetAllCommand buildClusteredGetAllCommand(List<?> keys, long flagsBitSet, GlobalTransaction gtx) {
      return new ClusteredGetAllCommand(cacheName, keys, flagsBitSet, gtx);
   }

   private CommandInvocationId generateUUID(boolean tx) {
      if (tx) {
         return CommandInvocationId.DUMMY_INVOCATION_ID;
      } else {
         return CommandInvocationId.generateId(clusteringDependentLogic.getAddress());
      }
   }

   @Override
   public <K, V, R> ReadOnlyKeyCommand<K, V, R> buildReadOnlyKeyCommand(Object key, Function<ReadEntryView<K, V>, R> f,
         int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new ReadOnlyKeyCommand<>(key, f, segment, params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, R> ReadOnlyManyCommand<K, V, R> buildReadOnlyManyCommand(Collection<?> keys, Function<ReadEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new ReadOnlyManyCommand<>(keys, f, params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, T, R> ReadWriteKeyValueCommand<K, V, T, R> buildReadWriteKeyValueCommand(Object key, Object argument,
         BiFunction<T, ReadWriteEntryView<K, V>, R> f, int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new ReadWriteKeyValueCommand<>(key, argument, f, segment, generateUUID(transactional), ValueMatcher.MATCH_ALWAYS,
            params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, R> ReadWriteKeyCommand<K, V, R> buildReadWriteKeyCommand(Object key,
         Function<ReadWriteEntryView<K, V>, R> f, int segment, Params params, DataConversion keyDataConversion,
         DataConversion valueDataConversion) {
      return init(new ReadWriteKeyCommand<>(key, f, segment, generateUUID(transactional), ValueMatcher.MATCH_ALWAYS, params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, R> ReadWriteManyCommand<K, V, R> buildReadWriteManyCommand(Collection<?> keys, Function<ReadWriteEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new ReadWriteManyCommand<>(keys, f, params, generateUUID(transactional), keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, T, R> ReadWriteManyEntriesCommand<K, V, T, R> buildReadWriteManyEntriesCommand(Map<?, ?> entries, BiFunction<T, ReadWriteEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new ReadWriteManyEntriesCommand<>(entries, f, params, generateUUID(transactional), keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V> WriteOnlyKeyCommand<K, V> buildWriteOnlyKeyCommand(
         Object key, Consumer<WriteEntryView<K, V>> f, int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new WriteOnlyKeyCommand<>(key, f, segment, generateUUID(transactional), ValueMatcher.MATCH_ALWAYS, params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, T> WriteOnlyKeyValueCommand<K, V, T> buildWriteOnlyKeyValueCommand(Object key, Object argument, BiConsumer<T, WriteEntryView<K, V>> f,
         int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new WriteOnlyKeyValueCommand<>(key, argument, f, segment, generateUUID(transactional), ValueMatcher.MATCH_ALWAYS, params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V> WriteOnlyManyCommand<K, V> buildWriteOnlyManyCommand(Collection<?> keys, Consumer<WriteEntryView<K, V>> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new WriteOnlyManyCommand<>(keys, f, params, generateUUID(transactional), keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, T> WriteOnlyManyEntriesCommand<K, V, T> buildWriteOnlyManyEntriesCommand(
         Map<?, ?> arguments, BiConsumer<T, WriteEntryView<K, V>> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new WriteOnlyManyEntriesCommand<>(arguments, f, params, generateUUID(transactional), keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, R> TxReadOnlyKeyCommand<K, V, R> buildTxReadOnlyKeyCommand(Object key, Function<ReadEntryView<K, V>, R> f, List<Mutation<K, V, ?>> mutations, int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new TxReadOnlyKeyCommand<>(key, f, mutations, segment, params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, R> TxReadOnlyManyCommand<K, V, R> buildTxReadOnlyManyCommand(Collection<?> keys, List<List<Mutation<K, V, ?>>> mutations,
                                                                              Params params, DataConversion keyDataConversion,
                                                                              DataConversion valueDataConversion) {
      return init(new TxReadOnlyManyCommand<K, V, R>(keys, mutations, params, keyDataConversion, valueDataConversion));
   }

   @Override
   public BackupMultiKeyAckCommand buildBackupMultiKeyAckCommand(long id, int segment, int topologyId) {
      return new BackupMultiKeyAckCommand(cacheName, id, segment, topologyId);
   }

   @Override
   public ExceptionAckCommand buildExceptionAckCommand(long id, Throwable throwable, int topologyId) {
      return new ExceptionAckCommand(cacheName, id, throwable, topologyId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(IracPutKeyValueCommand command, long sequence, int segmentId) {
      return SingleKeyBackupWriteCommand.create(cacheName, command, sequence, segmentId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(PutKeyValueCommand command, long sequence, int segmentId) {
      return SingleKeyBackupWriteCommand.create(cacheName, command, sequence, segmentId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(RemoveCommand command, long sequence, int segmentId) {
      return SingleKeyBackupWriteCommand.create(cacheName, command, sequence, segmentId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(ReplaceCommand command, long sequence, int segmentId) {
      return SingleKeyBackupWriteCommand.create(cacheName, command, sequence, segmentId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(ComputeIfAbsentCommand command, long sequence, int segmentId) {
      return SingleKeyBackupWriteCommand.create(cacheName, command, sequence, segmentId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(ComputeCommand command, long sequence, int segmentId) {
      return SingleKeyBackupWriteCommand.create(cacheName, command, sequence, segmentId);
   }

   @Override
   public <K, V, T, R> SingleKeyFunctionalBackupWriteCommand buildSingleKeyBackupWriteCommand(ReadWriteKeyValueCommand<K, V, T, R> command, long sequence, int segmentId) {
      return SingleKeyFunctionalBackupWriteCommand.create(cacheName, command, sequence, segmentId);
   }

   @Override
   public <K, V, R> SingleKeyFunctionalBackupWriteCommand buildSingleKeyBackupWriteCommand(ReadWriteKeyCommand<K, V, R> command, long sequence, int segmentId) {
      return SingleKeyFunctionalBackupWriteCommand.create(cacheName, command, sequence, segmentId);
   }

   @Override
   public <K, V> SingleKeyFunctionalBackupWriteCommand buildSingleKeyBackupWriteCommand(WriteOnlyKeyCommand<K, V> command, long sequence, int segmentId) {
      return SingleKeyFunctionalBackupWriteCommand.create(cacheName, command, sequence, segmentId);
   }

   @Override
   public <K, V, T> SingleKeyFunctionalBackupWriteCommand buildSingleKeyBackupWriteCommand(WriteOnlyKeyValueCommand<K, V, T> command, long sequence, int segmentId) {
      return SingleKeyFunctionalBackupWriteCommand.create(cacheName, command, sequence, segmentId);
   }

   @Override
   public PutMapBackupWriteCommand buildPutMapBackupWriteCommand(PutMapCommand command, Collection<Object> keys, long sequence, int segmentId) {
      return new PutMapBackupWriteCommand(cacheName, command, sequence, segmentId, keys);
   }

   @Override
   public <K, V, T> MultiEntriesFunctionalBackupWriteCommand buildMultiEntriesFunctionalBackupWriteCommand(WriteOnlyManyEntriesCommand<K, V, T> command, Collection<Object> keys, long sequence, int segmentId) {
      return MultiEntriesFunctionalBackupWriteCommand.create(cacheName, command, keys, sequence, segmentId);
   }

   @Override
   public <K, V, T, R> MultiEntriesFunctionalBackupWriteCommand buildMultiEntriesFunctionalBackupWriteCommand(ReadWriteManyEntriesCommand<K, V, T, R> command, Collection<Object> keys, long sequence, int segmentId) {
      return MultiEntriesFunctionalBackupWriteCommand.create(cacheName, command, keys, sequence, segmentId);
   }

   @Override
   public <K, V> MultiKeyFunctionalBackupWriteCommand buildMultiKeyFunctionalBackupWriteCommand(WriteOnlyManyCommand<K, V> command, Collection<Object> keys, long sequence, int segmentId) {
      return MultiKeyFunctionalBackupWriteCommand.create(cacheName, command, keys, sequence, segmentId);
   }

   @Override
   public <K, V, R> MultiKeyFunctionalBackupWriteCommand buildMultiKeyFunctionalBackupWriteCommand(ReadWriteManyCommand<K, V, R> command, Collection<Object> keys, long sequence, int segmentId) {
      return MultiKeyFunctionalBackupWriteCommand.create(cacheName, command, keys, sequence, segmentId);
   }

   @Override
   public BackupNoopCommand buildBackupNoopCommand(WriteCommand command, long sequence, int segmentId) {
      return new BackupNoopCommand(cacheName, command, sequence, segmentId);
   }

   @Override
   public <K, R> ReductionPublisherRequestCommand<K> buildKeyReductionPublisherCommand(boolean parallelStream,
         DeliveryGuarantee deliveryGuarantee, IntSet segments, Set<K> keys, Set<K> excludedKeys, long explicitFlags,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return new ReductionPublisherRequestCommand<>(cacheName, parallelStream, deliveryGuarantee, segments, keys, excludedKeys,
            explicitFlags, false, transformer, finalizer);
   }

   @Override
   public <K, V, R> ReductionPublisherRequestCommand<K> buildEntryReductionPublisherCommand(boolean parallelStream,
         DeliveryGuarantee deliveryGuarantee, IntSet segments, Set<K> keys, Set<K> excludedKeys, long explicitFlags,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return new ReductionPublisherRequestCommand<>(cacheName, parallelStream, deliveryGuarantee, segments, keys, excludedKeys,
            explicitFlags, true, transformer, finalizer);
   }

   @Override
   public <K, I, R> InitialPublisherCommand<K, I, R> buildInitialPublisherCommand(String requestId, DeliveryGuarantee deliveryGuarantee,
                                                                                  int batchSize, IntSet segments, Set<K> keys, Set<K> excludedKeys, long explicitFlags, boolean entryStream,
                                                                                  boolean trackKeys, Function<? super Publisher<I>, ? extends Publisher<R>> transformer) {
      return new InitialPublisherCommand<>(cacheName, requestId, deliveryGuarantee, batchSize, segments, keys,
            excludedKeys, explicitFlags, entryStream, trackKeys, transformer);
   }

   @Override
   public NextPublisherCommand buildNextPublisherCommand(String requestId) {
      return new NextPublisherCommand(cacheName, requestId);
   }

   @Override
   public CancelPublisherCommand buildCancelPublisherCommand(String requestId) {
      return new CancelPublisherCommand(cacheName, requestId);
   }

   @Override
   public <K, V> MultiClusterEventCommand<K, V> buildMultiClusterEventCommand(Map<UUID, Collection<ClusterEvent<K, V>>> events) {
      return new MultiClusterEventCommand<>(cacheName, events);
   }

   @Override
   public CheckTransactionRpcCommand buildCheckTransactionRpcCommand(Collection<GlobalTransaction> globalTransactions) {
      return new CheckTransactionRpcCommand(cacheName, globalTransactions);
   }

   @Override
   public TouchCommand buildTouchCommand(Object key, int segment, boolean touchEvenIfExpired, long flagBitSet) {
      return new TouchCommand(key, segment, flagBitSet, touchEvenIfExpired);
   }

   @Override
   public IracClearKeysRequest buildIracClearKeysCommand() {
      return new IracClearKeysRequest(cacheName);
   }

   @Override
   public IracCleanupKeysCommand buildIracCleanupKeyCommand(Collection<IracManagerKeyInfo> state) {
      return new IracCleanupKeysCommand(cacheName, state);
   }

   @Override
   public IracTombstoneCleanupCommand buildIracTombstoneCleanupCommand(int maxCapacity) {
      return new IracTombstoneCleanupCommand(cacheName, maxCapacity);
   }

   @Override
   public IracMetadataRequestCommand buildIracMetadataRequestCommand(int segment, IracEntryVersion versionSeen) {
      return new IracMetadataRequestCommand(cacheName, segment, versionSeen);
   }

   @Override
   public IracRequestStateCommand buildIracRequestStateCommand(IntSet segments) {
      return new IracRequestStateCommand(cacheName, segments);
   }

   @Override
   public IracStateResponseCommand buildIracStateResponseCommand(int capacity) {
      return new IracStateResponseCommand(cacheName, capacity);
   }

   @Override
   public IracPutKeyValueCommand buildIracPutKeyValueCommand(Object key, int segment, Object value, Metadata metadata,
         PrivateMetadata privateMetadata) {
      return new IracPutKeyValueCommand(key, segment, generateUUID(false), value, metadata, privateMetadata);
   }

   @Override
   public IracTouchKeyRequest buildIracTouchCommand(Object key) {
      return new IracTouchKeyRequest(cacheName, key);
   }

   @Override
   public IracUpdateVersionCommand buildIracUpdateVersionCommand(Map<Integer, IracEntryVersion> segmentsVersion) {
      return new IracUpdateVersionCommand(cacheName, segmentsVersion);
   }

   @Override
   public XSiteAutoTransferStatusCommand buildXSiteAutoTransferStatusCommand(String site) {
      return new XSiteAutoTransferStatusCommand(cacheName, site);
   }

   @Override
   public XSiteSetStateTransferModeCommand buildXSiteSetStateTransferModeCommand(String site, XSiteStateTransferMode mode) {
      return new XSiteSetStateTransferModeCommand(cacheName, site, mode);
   }

   @Override
   public IracTombstoneRemoteSiteCheckCommand buildIracTombstoneRemoteSiteCheckCommand(List<Object> keys) {
      return new IracTombstoneRemoteSiteCheckCommand(cacheName, keys);
   }

   @Override
   public IracTombstoneStateResponseCommand buildIracTombstoneStateResponseCommand(Collection<IracTombstoneInfo> state) {
      return new IracTombstoneStateResponseCommand(cacheName, state);
   }

   @Override
   public IracTombstonePrimaryCheckCommand buildIracTombstonePrimaryCheckCommand(Collection<IracTombstoneInfo> tombstones) {
      return new IracTombstonePrimaryCheckCommand(cacheName, tombstones);
   }

   @Override
   public IracPutManyRequest buildIracPutManyCommand(int capacity) {
      return new IracPutManyRequest(cacheName, capacity);
   }

   @Override
   public XSiteStatePushRequest buildXSiteStatePushRequest(List<XSiteState> chunk, long timeoutMillis) {
      return new XSiteStatePushRequest(cacheName, chunk, timeoutMillis);
   }

   @Override
   public IracTombstoneCheckRequest buildIracTombstoneCheckRequest(List<Object> keys) {
      return new IracTombstoneCheckRequest(cacheName, keys);
   }

   @Override
   public IracPrimaryPendingKeyCheckCommand buildIracPrimaryPendingKeyCheckCommand(List<IracManagerKeyInfo> keys) {
      return new IracPrimaryPendingKeyCheckCommand(cacheName, keys);
   }
}
