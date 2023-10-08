package org.infinispan.util.mocks;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
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
import org.infinispan.commands.write.IracPutKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.XSiteStateTransferMode;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracTombstoneInfo;
import org.infinispan.encoding.DataConversion;
import org.infinispan.expiration.impl.TouchCommand;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.impl.Params;
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
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.ReclosableLatch;
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
 * @author Mircea Markus
 * @since 5.2
 */
public class ControlledCommandFactory implements CommandsFactory {

   private final static Log log = LogFactory.getLog(ControlledCommandFactory.class);

   public final CommandsFactory actual;
   public final ReclosableLatch gate = new ReclosableLatch(true);
   public final AtomicInteger remoteCommandsReceived = new AtomicInteger(0);
   public final AtomicInteger blockTypeCommandsReceived = new AtomicInteger(0);
   public final List<ReplicableCommand> receivedCommands = new ArrayList<>();
   public final Class<? extends  ReplicableCommand> toBlock;

   public ControlledCommandFactory(CommandsFactory actual, Class<? extends ReplicableCommand> toBlock) {
      this.actual = actual;
      this.toBlock = toBlock;
   }

   public int received(Class<? extends ReplicableCommand> command) {
      int result = 0;
      for (ReplicableCommand r : receivedCommands) {
         if (r.getClass() == command) {
            result++;
         }
      }
      return result;
   }

   @Override
   public void initializeReplicableCommand(ReplicableCommand command, boolean isRemote) {
      log.tracef("Received command %s", command);
      receivedCommands.add(command);
      if (isRemote) {
         remoteCommandsReceived.incrementAndGet();
         if (toBlock != null && command.getClass().isAssignableFrom(toBlock)) {
            blockTypeCommandsReceived.incrementAndGet();
            try {
               gate.await(30, TimeUnit.SECONDS);
               log.tracef("gate is opened, processing the lock cleanup:  %s", command);
            } catch (InterruptedException e) {
               throw new RuntimeException(e);
            }
         }
      }
      actual.initializeReplicableCommand(command, isRemote);
   }

   public static ControlledCommandFactory registerControlledCommandFactory(Cache cache, Class<? extends ReplicableCommand> toBlock) {
      ComponentRegistry componentRegistry =  ComponentRegistry.of(cache);
      final ControlledCommandFactory ccf = new ControlledCommandFactory(componentRegistry.getCommandsFactory(), toBlock);
      TestingUtil.replaceComponent(cache, CommandsFactory.class, ccf, true);

      //hack: re-add the component registry to the GlobalComponentRegistry's "namedComponents" (CHM) in order to correctly publish it for
      // when it will be read by the InboundInvocationHandlder. InboundInvocationHandlder reads the value from the GlobalComponentRegistry.namedComponents before using it
      componentRegistry.getGlobalComponentRegistry().registerNamedComponentRegistry(componentRegistry, TestingUtil.getDefaultCacheName(cache.getCacheManager()));
      return ccf;
   }

   @Override
   public PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, int segment, Metadata metadata,
                                                     long flagsBitSet, boolean returnEntry) {
      return actual.buildPutKeyValueCommand(key, value, segment, metadata, flagsBitSet, returnEntry);
   }

   @Override
   public RemoveCommand buildRemoveCommand(Object key, Object value, int segment, long flagsBitSet, boolean returnEntry) {
      return actual.buildRemoveCommand(key, value, segment, flagsBitSet, returnEntry);
   }

   @Override
   public InvalidateCommand buildInvalidateCommand(long flagsBitSet, Object... keys) {
      return actual.buildInvalidateCommand(flagsBitSet, keys);
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(long flagsBitSet, Collection<Object> keys) {
      return actual.buildInvalidateFromL1Command(flagsBitSet, keys);
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(Address origin, long flagsBitSet, Collection<Object> keys) {
      return actual.buildInvalidateFromL1Command(origin, flagsBitSet, keys);
   }

   @Override
   public RemoveExpiredCommand buildRemoveExpiredCommand(Object key, Object value, int segment, Long lifespan,
         long flagsBitSet) {
      return actual.buildRemoveExpiredCommand(key, value, segment, lifespan, flagsBitSet);
   }

   @Override
   public RemoveExpiredCommand buildRemoveExpiredCommand(Object key, Object value, int segment, long flagsBitSet) {
      return actual.buildRemoveExpiredCommand(key, value, segment, flagsBitSet);
   }

   @Override
   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, int segment,
                                             Metadata metadata, long flagsBitSet, boolean loadEntry) {
      return actual.buildReplaceCommand(key, oldValue, newValue, segment, metadata, flagsBitSet, loadEntry);
   }

   @Override
   public ComputeCommand buildComputeCommand(Object key, BiFunction mappingFunction, boolean computeIfPresent,
         int segment, Metadata metadata, long flagsBitSet) {
      return actual.buildComputeCommand(key, mappingFunction, computeIfPresent, segment, metadata, flagsBitSet);
   }

   @Override
   public ComputeIfAbsentCommand buildComputeIfAbsentCommand(Object key, Function mappingFunction, int segment,
         Metadata metadata, long flagsBitSet) {
      return actual.buildComputeIfAbsentCommand(key, mappingFunction, segment, metadata, flagsBitSet);
   }

   @Override
   public SizeCommand buildSizeCommand(IntSet segments, long flagsBitSet) {
      return actual.buildSizeCommand(segments, flagsBitSet);
   }

   @Override
   public GetKeyValueCommand buildGetKeyValueCommand(Object key, int segment, long flagsBitSet) {
      return actual.buildGetKeyValueCommand(key, segment, flagsBitSet);
   }

   @Override
   public GetAllCommand buildGetAllCommand(Collection<?> keys, long flagsBitSet, boolean returnEntries) {
      return actual.buildGetAllCommand(keys, flagsBitSet, returnEntries);
   }

   @Override
   public KeySetCommand buildKeySetCommand(long flagsBitSet) {
      return actual.buildKeySetCommand(flagsBitSet);
   }

   @Override
   public EntrySetCommand buildEntrySetCommand(long flagsBitSet) {
      return actual.buildEntrySetCommand(flagsBitSet);
   }

   @Override
   public PutMapCommand buildPutMapCommand(Map<?, ?> map, Metadata metadata, long flagsBitSet) {
      return actual.buildPutMapCommand(map, metadata, flagsBitSet);
   }

   @Override
   public ClearCommand buildClearCommand(long flagsBitSet) {
      return actual.buildClearCommand(flagsBitSet);
   }

   @Override
   public EvictCommand buildEvictCommand(Object key, int segment, long flagsBitSet) {
      return actual.buildEvictCommand(key, segment, flagsBitSet);
   }

   @Override
   public PrepareCommand buildPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit) {
      return actual.buildPrepareCommand(gtx, modifications, onePhaseCommit);
   }

   @Override
   public VersionedPrepareCommand buildVersionedPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhase) {
      return actual.buildVersionedPrepareCommand(gtx, modifications, onePhase);
   }

   @Override
   public CommitCommand buildCommitCommand(GlobalTransaction gtx) {
      return actual.buildCommitCommand(gtx);
   }

   @Override
   public VersionedCommitCommand buildVersionedCommitCommand(GlobalTransaction gtx) {
      return actual.buildVersionedCommitCommand(gtx);
   }

   @Override
   public RollbackCommand buildRollbackCommand(GlobalTransaction gtx) {
      return actual.buildRollbackCommand(gtx);
   }

   @Override
   public SingleRpcCommand buildSingleRpcCommand(VisitableCommand call) {
      return actual.buildSingleRpcCommand(call);
   }

   @Override
   public ClusteredGetCommand buildClusteredGetCommand(Object key, Integer segment, long flagsBitSet) {
      return actual.buildClusteredGetCommand(key, segment, flagsBitSet);
   }

   @Override
   public ClusteredGetAllCommand buildClusteredGetAllCommand(List<?> keys, long flagsBitSet, GlobalTransaction gtx) {
      return actual.buildClusteredGetAllCommand(keys, flagsBitSet, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection<?> keys, long flagsBitSet, GlobalTransaction gtx) {
      return actual.buildLockControlCommand(keys, flagsBitSet, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Object key, long flagsBitSet, GlobalTransaction gtx) {
      return actual.buildLockControlCommand(key, flagsBitSet, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection<?> keys, long flagsBitSet) {
      return actual.buildLockControlCommand(keys, flagsBitSet);
   }

   @Override
   public ConflictResolutionStartCommand buildConflictResolutionStartCommand(int topologyId, IntSet segments) {
      return actual.buildConflictResolutionStartCommand(topologyId, segments);
   }

   @Override
   public StateTransferCancelCommand buildStateTransferCancelCommand(int topologyId, IntSet segments) {
      return actual.buildStateTransferCancelCommand(topologyId, segments);
   }

   @Override
   public StateTransferGetListenersCommand buildStateTransferGetListenersCommand(int topologyId) {
      return actual.buildStateTransferGetListenersCommand(topologyId);
   }

   @Override
   public StateTransferGetTransactionsCommand buildStateTransferGetTransactionsCommand(int topologyId, IntSet segments) {
      return actual.buildStateTransferGetTransactionsCommand(topologyId, segments);
   }

   @Override
   public StateTransferStartCommand buildStateTransferStartCommand(int topologyId, IntSet segments) {
      return actual.buildStateTransferStartCommand(topologyId, segments);
   }

   @Override
   public StateResponseCommand buildStateResponseCommand(int viewId, Collection<StateChunk> stateChunks, boolean applyState) {
      return actual.buildStateResponseCommand(viewId, stateChunks, applyState);
   }

   @Override
   public String getCacheName() {
      return actual.getCacheName();
   }

   @Override
   public GetInDoubtTransactionsCommand buildGetInDoubtTransactionsCommand() {
      return actual.buildGetInDoubtTransactionsCommand();
   }

   @Override
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(XidImpl xid, GlobalTransaction globalTransaction) {
      return actual.buildTxCompletionNotificationCommand(xid, globalTransaction);
   }

   @Override
   public GetInDoubtTxInfoCommand buildGetInDoubtTxInfoCommand() {
      return actual.buildGetInDoubtTxInfoCommand();
   }

   @Override
   public CompleteTransactionCommand buildCompleteTransactionCommand(XidImpl xid, boolean commit) {
      return actual.buildCompleteTransactionCommand(xid, commit);
   }

   @Override
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(long internalId) {
      return actual.buildTxCompletionNotificationCommand(internalId);
   }

   @Override
   public XSiteStateTransferCancelSendCommand buildXSiteStateTransferCancelSendCommand(String siteName) {
      return actual.buildXSiteStateTransferCancelSendCommand(siteName);
   }

   @Override
   public XSiteStateTransferClearStatusCommand buildXSiteStateTransferClearStatusCommand() {
      return actual.buildXSiteStateTransferClearStatusCommand();
   }

   @Override
   public XSiteStateTransferFinishReceiveCommand buildXSiteStateTransferFinishReceiveCommand(String siteName) {
      return actual.buildXSiteStateTransferFinishReceiveCommand(siteName);
   }

   @Override
   public XSiteStateTransferFinishSendCommand buildXSiteStateTransferFinishSendCommand(String siteName, boolean statusOk) {
      return actual.buildXSiteStateTransferFinishSendCommand(siteName, statusOk);
   }

   @Override
   public XSiteStateTransferRestartSendingCommand buildXSiteStateTransferRestartSendingCommand(String siteName, int topologyId) {
      return actual.buildXSiteStateTransferRestartSendingCommand(siteName, topologyId);
   }

   @Override
   public XSiteStateTransferStartReceiveCommand buildXSiteStateTransferStartReceiveCommand(String siteName) {
      return actual.buildXSiteStateTransferStartReceiveCommand(siteName);
   }

   @Override
   public XSiteStateTransferStartSendCommand buildXSiteStateTransferStartSendCommand(String siteName, int topologyId) {
      return actual.buildXSiteStateTransferStartSendCommand(siteName, topologyId);
   }

   @Override
   public XSiteStateTransferStatusRequestCommand buildXSiteStateTransferStatusRequestCommand() {
      return actual.buildXSiteStateTransferStatusRequestCommand();
   }

   @Override
   public XSiteAmendOfflineStatusCommand buildXSiteAmendOfflineStatusCommand(String siteName, Integer afterFailures, Long minTimeToWait) {
      return actual.buildXSiteAmendOfflineStatusCommand(siteName, afterFailures, minTimeToWait);
   }

   @Override
   public XSiteStatePushCommand buildXSiteStatePushCommand(XSiteState[] chunk) {
      return actual.buildXSiteStatePushCommand(chunk);
   }

   @Override
   public SingleXSiteRpcCommand buildSingleXSiteRpcCommand(VisitableCommand command) {
      return actual.buildSingleXSiteRpcCommand(command);
   }

   @Override
   public GetCacheEntryCommand buildGetCacheEntryCommand(Object key, int segment, long flagsBitSet) {
      return actual.buildGetCacheEntryCommand(key, segment, flagsBitSet);
   }

   @Override
   public <K, V, R> ReadOnlyKeyCommand<K, V, R> buildReadOnlyKeyCommand(Object key, Function<EntryView.ReadEntryView<K, V>, R> f,
         int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildReadOnlyKeyCommand(key, f, segment, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, R> ReadOnlyManyCommand<K, V, R> buildReadOnlyManyCommand(Collection<?> keys, Function<EntryView.ReadEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildReadOnlyManyCommand(keys, f, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, T, R> ReadWriteKeyValueCommand<K, V, T, R> buildReadWriteKeyValueCommand(Object key, Object argument, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f,
         int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildReadWriteKeyValueCommand(key, argument, f, segment, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, R> ReadWriteKeyCommand<K, V, R> buildReadWriteKeyCommand(
         Object key, Function<EntryView.ReadWriteEntryView<K, V>, R> f, int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildReadWriteKeyCommand(key, f, segment, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, R> ReadWriteManyCommand<K, V, R> buildReadWriteManyCommand(Collection<?> keys, Function<EntryView.ReadWriteEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildReadWriteManyCommand(keys, f, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, T, R> ReadWriteManyEntriesCommand<K, V, T, R> buildReadWriteManyEntriesCommand(Map<?, ?> entries, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildReadWriteManyEntriesCommand(entries, f, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V> WriteOnlyKeyCommand<K, V> buildWriteOnlyKeyCommand(
         Object key, Consumer<EntryView.WriteEntryView<K, V>> f, int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildWriteOnlyKeyCommand(key, f, segment, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, T> WriteOnlyKeyValueCommand<K, V, T> buildWriteOnlyKeyValueCommand(Object key, Object argument, BiConsumer<T, EntryView.WriteEntryView<K, V>> f,
         int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildWriteOnlyKeyValueCommand(key, argument, f, segment, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V> WriteOnlyManyCommand<K, V> buildWriteOnlyManyCommand(Collection<?> keys, Consumer<EntryView.WriteEntryView<K, V>> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildWriteOnlyManyCommand(keys, f, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, T> WriteOnlyManyEntriesCommand<K, V, T> buildWriteOnlyManyEntriesCommand(
         Map<?, ?> arguments, BiConsumer<T, EntryView.WriteEntryView<K, V>> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildWriteOnlyManyEntriesCommand(arguments, f, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, R> TxReadOnlyKeyCommand<K, V, R> buildTxReadOnlyKeyCommand(Object key, Function<EntryView.ReadEntryView<K, V>, R> f, List<Mutation<K, V, ?>> mutations, int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildTxReadOnlyKeyCommand(key, f, mutations, segment, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, R> TxReadOnlyManyCommand<K, V, R> buildTxReadOnlyManyCommand(Collection<?> keys, List<List<Mutation<K, V, ?>>> mutations, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildTxReadOnlyManyCommand(keys, mutations, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public BackupMultiKeyAckCommand buildBackupMultiKeyAckCommand(long id, int segment, int topologyId) {
      return actual.buildBackupMultiKeyAckCommand(id, segment, topologyId);
   }

   @Override
   public ExceptionAckCommand buildExceptionAckCommand(long id, Throwable throwable, int topologyId) {
      return actual.buildExceptionAckCommand(id, throwable, topologyId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(ReplaceCommand command, long sequenceId, int segmentId) {
      return actual.buildSingleKeyBackupWriteCommand(command, sequenceId, segmentId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(ComputeIfAbsentCommand command, long sequenceId, int segmentId) {
      return actual.buildSingleKeyBackupWriteCommand(command, sequenceId, segmentId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(ComputeCommand command, long sequenceId, int segmentId) {
      return actual.buildSingleKeyBackupWriteCommand(command, sequenceId, segmentId);
   }

   @Override
   public <K, V, T, R> SingleKeyFunctionalBackupWriteCommand buildSingleKeyBackupWriteCommand(ReadWriteKeyValueCommand<K, V, T, R> command, long sequenceId, int segmentId) {
      return actual.buildSingleKeyBackupWriteCommand(command, sequenceId, segmentId);
   }

   @Override
   public <K, V, R> SingleKeyFunctionalBackupWriteCommand buildSingleKeyBackupWriteCommand(ReadWriteKeyCommand<K, V, R> command, long sequenceId, int segmentId) {
      return actual.buildSingleKeyBackupWriteCommand(command, sequenceId, segmentId);
   }

   @Override
   public <K, V> SingleKeyFunctionalBackupWriteCommand buildSingleKeyBackupWriteCommand(WriteOnlyKeyCommand<K, V> command, long sequenceId, int segmentId) {
      return actual.buildSingleKeyBackupWriteCommand(command, sequenceId, segmentId);
   }

   @Override
   public <K, V, T> SingleKeyFunctionalBackupWriteCommand buildSingleKeyBackupWriteCommand(WriteOnlyKeyValueCommand<K, V, T> command, long sequenceId, int segmentId) {
      return actual.buildSingleKeyBackupWriteCommand(command, sequenceId, segmentId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(PutKeyValueCommand command, long sequence, int segmentId) {
      return actual.buildSingleKeyBackupWriteCommand(command, sequence, segmentId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(IracPutKeyValueCommand command, long sequence, int segmentId) {
      return actual.buildSingleKeyBackupWriteCommand(command, sequence, segmentId);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand(RemoveCommand command, long sequence, int segmentId) {
      return actual.buildSingleKeyBackupWriteCommand(command, sequence, segmentId);
   }

   @Override
   public PutMapBackupWriteCommand buildPutMapBackupWriteCommand(PutMapCommand command, Collection<Object> keys, long sequenceId, int segmentId) {
      return actual.buildPutMapBackupWriteCommand(command, keys, sequenceId, segmentId);
   }

   @Override
   public <K, V, T> MultiEntriesFunctionalBackupWriteCommand buildMultiEntriesFunctionalBackupWriteCommand(WriteOnlyManyEntriesCommand<K, V, T> command, Collection<Object> keys, long sequenceId, int segmendId) {
      return actual.buildMultiEntriesFunctionalBackupWriteCommand(command, keys, sequenceId, segmendId);
   }

   @Override
   public <K, V, T, R> MultiEntriesFunctionalBackupWriteCommand buildMultiEntriesFunctionalBackupWriteCommand(ReadWriteManyEntriesCommand<K, V, T, R> command, Collection<Object> keys, long sequenceId, int segmendId) {
      return actual.buildMultiEntriesFunctionalBackupWriteCommand(command, keys, sequenceId, segmendId);
   }

   @Override
   public <K, V> MultiKeyFunctionalBackupWriteCommand buildMultiKeyFunctionalBackupWriteCommand(WriteOnlyManyCommand<K, V> command, Collection<Object> keys, long sequenceId, int segmendId) {
      return actual.buildMultiKeyFunctionalBackupWriteCommand(command, keys, sequenceId, segmendId);
   }

   @Override
   public <K, V, R> MultiKeyFunctionalBackupWriteCommand buildMultiKeyFunctionalBackupWriteCommand(ReadWriteManyCommand<K, V, R> command, Collection<Object> keys, long sequenceId, int segmendId) {
      return actual.buildMultiKeyFunctionalBackupWriteCommand(command, keys, sequenceId, segmendId);
   }

   @Override
   public BackupNoopCommand buildBackupNoopCommand(WriteCommand command, long sequence, int segmentId) {
      return actual.buildBackupNoopCommand(command, sequence, segmentId);
   }

   @Override
   public <K, R> ReductionPublisherRequestCommand<K> buildKeyReductionPublisherCommand(boolean parallelStream,
         DeliveryGuarantee deliveryGuarantee, IntSet segments, Set<K> keys, Set<K> excludedKeys, long explicitFlags,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return actual.buildKeyReductionPublisherCommand(parallelStream, deliveryGuarantee, segments, keys, excludedKeys,
            explicitFlags, transformer, finalizer);
   }

   @Override
   public <K, V, R> ReductionPublisherRequestCommand<K> buildEntryReductionPublisherCommand(boolean parallelStream, DeliveryGuarantee deliveryGuarantee, IntSet segments, Set<K> keys, Set<K> excludedKeys, long explicitFlags, Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer, Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return actual.buildEntryReductionPublisherCommand(parallelStream, deliveryGuarantee, segments, keys, excludedKeys,
            explicitFlags, transformer, finalizer);
   }

   @Override
   public <K, I, R> InitialPublisherCommand<K, I, R> buildInitialPublisherCommand(String requestId, DeliveryGuarantee deliveryGuarantee, int batchSize, IntSet segments, Set<K> keys, Set<K> excludedKeys, long explicitFlags, boolean entryStream, boolean trackKeys, Function<? super Publisher<I>, ? extends Publisher<R>> transformer) {
      return actual.buildInitialPublisherCommand(requestId, deliveryGuarantee, batchSize, segments, keys, excludedKeys, explicitFlags, entryStream, trackKeys, transformer);
   }

   @Override
   public NextPublisherCommand buildNextPublisherCommand(String requestId) {
      return actual.buildNextPublisherCommand(requestId);
   }

   @Override
   public CancelPublisherCommand buildCancelPublisherCommand(String requestId) {
      return actual.buildCancelPublisherCommand(requestId);
   }

   @Override
   public <K, V> MultiClusterEventCommand<K, V> buildMultiClusterEventCommand(Map<UUID, Collection<ClusterEvent<K, V>>> events) {
      return actual.buildMultiClusterEventCommand(events);
   }

   @Override
   public CheckTransactionRpcCommand buildCheckTransactionRpcCommand(Collection<GlobalTransaction> globalTransactions) {
      return actual.buildCheckTransactionRpcCommand(globalTransactions);
   }

   @Override
   public TouchCommand buildTouchCommand(Object key, int segment, boolean touchEvenIfExpired, long flagBitSet) {
      return actual.buildTouchCommand(key, segment, touchEvenIfExpired, flagBitSet);
   }

   @Override
   public IracClearKeysRequest buildIracClearKeysCommand() {
      return actual.buildIracClearKeysCommand();
   }

   @Override
   public IracCleanupKeysCommand buildIracCleanupKeyCommand(Collection<IracManagerKeyInfo> state) {
      return actual.buildIracCleanupKeyCommand(state);
   }

   @Override
   public IracTombstoneCleanupCommand buildIracTombstoneCleanupCommand(int maxCapacity) {
      return actual.buildIracTombstoneCleanupCommand(maxCapacity);
   }

   @Override
   public IracMetadataRequestCommand buildIracMetadataRequestCommand(int segment, IracEntryVersion versionSeen) {
      return actual.buildIracMetadataRequestCommand(segment, versionSeen);
   }

   @Override
   public IracRequestStateCommand buildIracRequestStateCommand(IntSet segments) {
      return actual.buildIracRequestStateCommand(segments);
   }

   @Override
   public IracStateResponseCommand buildIracStateResponseCommand(int capacity) {
      return actual.buildIracStateResponseCommand(capacity);
   }

   @Override
   public IracPutKeyValueCommand buildIracPutKeyValueCommand(Object key, int segment, Object value, Metadata metadata,
         PrivateMetadata privateMetadata) {
      return actual.buildIracPutKeyValueCommand(key, segment, value, metadata, privateMetadata);
   }

   @Override
   public IracTouchKeyRequest buildIracTouchCommand(Object key) {
      return actual.buildIracTouchCommand(key);
   }

   @Override
   public IracUpdateVersionCommand buildIracUpdateVersionCommand(Map<Integer, IracEntryVersion> segmentsVersion) {
      return actual.buildIracUpdateVersionCommand(segmentsVersion);
   }

   @Override
   public XSiteAutoTransferStatusCommand buildXSiteAutoTransferStatusCommand(String site) {
      return actual.buildXSiteAutoTransferStatusCommand(site);
   }

   @Override
   public XSiteSetStateTransferModeCommand buildXSiteSetStateTransferModeCommand(String site, XSiteStateTransferMode mode) {
      return actual.buildXSiteSetStateTransferModeCommand(site, mode);
   }

   @Override
   public IracTombstoneRemoteSiteCheckCommand buildIracTombstoneRemoteSiteCheckCommand(List<Object> keys) {
      return actual.buildIracTombstoneRemoteSiteCheckCommand(keys);
   }

   @Override
   public IracTombstoneStateResponseCommand buildIracTombstoneStateResponseCommand(Collection<IracTombstoneInfo> state) {
      return actual.buildIracTombstoneStateResponseCommand(state);
   }

   @Override
   public IracTombstonePrimaryCheckCommand buildIracTombstonePrimaryCheckCommand(Collection<IracTombstoneInfo> tombstones) {
      return actual.buildIracTombstonePrimaryCheckCommand(tombstones);
   }

   @Override
   public IracPutManyRequest buildIracPutManyCommand(int capacity) {
      return actual.buildIracPutManyCommand(capacity);
   }

   @Override
   public XSiteStateTransferControlRequest buildXSiteStateTransferControlRequest(boolean startReceiving) {
      return actual.buildXSiteStateTransferControlRequest(startReceiving);
   }

   @Override
   public XSiteStatePushRequest buildXSiteStatePushRequest(XSiteState[] chunk, long timeoutMillis) {
      return actual.buildXSiteStatePushRequest(chunk, timeoutMillis);
   }

   @Override
   public IracTombstoneCheckRequest buildIracTombstoneCheckRequest(List<Object> keys) {
      return actual.buildIracTombstoneCheckRequest(keys);
   }

   @Override
   public IracPrimaryPendingKeyCheckCommand buildIracPrimaryPendingKeyCheckCommand(List<IracManagerKeyInfo> keys) {
      return actual.buildIracPrimaryPendingKeyCheckCommand(keys);
   }
}
