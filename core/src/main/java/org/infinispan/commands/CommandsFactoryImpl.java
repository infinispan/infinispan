package org.infinispan.commands;

import static org.infinispan.xsite.XSiteAdminCommand.AdminOperation;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand.StateTransferControl;

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

import javax.transaction.xa.Xid;

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
import org.infinispan.commands.module.ModuleCommandInitializer;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.SizeCommand;
import org.infinispan.commands.remote.ClusteredGetAllCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.remote.RenewBiasCommand;
import org.infinispan.commands.remote.RevokeBiasCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.expiration.RetrieveLastAccessCommand;
import org.infinispan.commands.remote.expiration.UpdateLastAccessCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
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
import org.infinispan.commands.tx.totalorder.TotalOrderCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderNonVersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderRollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedPrepareCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.InvalidateVersionsCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.marshall.Externalizer;
import org.infinispan.commons.marshall.LambdaExternalizer;
import org.infinispan.commons.marshall.SerializeFunctionWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.util.EnumUtil;
import org.infinispan.commons.util.IntSet;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.encoding.DataConversion;
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
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.cluster.ClusterEvent;
import org.infinispan.notifications.cachelistener.cluster.MultiClusterEventCommand;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.reactive.publisher.impl.commands.batch.CancelPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.InitialPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.batch.NextPublisherCommand;
import org.infinispan.reactive.publisher.impl.commands.reduction.ReductionPublisherRequestCommand;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.stream.impl.StreamIteratorCloseCommand;
import org.infinispan.stream.impl.StreamIteratorNextCommand;
import org.infinispan.stream.impl.StreamIteratorRequestCommand;
import org.infinispan.stream.impl.StreamRequestCommand;
import org.infinispan.stream.impl.StreamResponseCommand;
import org.infinispan.stream.impl.intops.IntermediateOperation;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.SingleXSiteRpcCommand;
import org.infinispan.xsite.XSiteAdminCommand;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;
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
   private static final boolean trace = log.isTraceEnabled();

   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject Configuration configuration;
   @Inject ComponentRef<Cache<Object, Object>> cache;
   @Inject ComponentRegistry componentRegistry;
   @Inject EmbeddedCacheManager cacheManager;
   @Inject @ComponentName(KnownComponentNames.INTERNAL_MARSHALLER)
   StreamingMarshaller marshaller;
   @Inject @ComponentName(KnownComponentNames.MODULE_COMMAND_INITIALIZERS)
   Map<Byte, ModuleCommandInitializer> moduleCommandInitializers;

   private ByteString cacheName;
   private boolean transactional;
   private boolean totalOrderProtocol;

   @Start(priority = 1)
   // needs to happen early on
   public void start() {
      cacheName = ByteString.fromString(cache.wired().getName());
      this.transactional = configuration.transaction().transactionMode().isTransactional();
      this.totalOrderProtocol = configuration.transaction().transactionProtocol().isTotalOrder();
   }

   @Override
   public PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, int segment, Metadata metadata,
         long flagsBitSet) {
      boolean reallyTransactional = transactional && !EnumUtil.containsAny(flagsBitSet, FlagBitSets.PUT_FOR_EXTERNAL_READ);
      return new PutKeyValueCommand(key, value, false, metadata, segment, flagsBitSet, generateUUID(reallyTransactional));
   }

   @Override
   public RemoveCommand buildRemoveCommand(Object key, Object value, int segment, long flagsBitSet) {
      return new RemoveCommand(key, value, segment, flagsBitSet, generateUUID(transactional));
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
            generateUUID(transactional));
   }

   @Override
   public RemoveExpiredCommand buildRemoveExpiredCommand(Object key, Object value, int segment, long flagsBitSet) {
      return new RemoveExpiredCommand(key, value, null, true, segment, flagsBitSet,
            generateUUID(transactional));
   }

   @Override
   public RetrieveLastAccessCommand buildRetrieveLastAccessCommand(Object key, Object value, int segment) {
      return new RetrieveLastAccessCommand(cacheName, key, value, segment);
   }

   @Override
   public UpdateLastAccessCommand buildUpdateLastAccessCommand(Object key, int segment, long accessTime) {
      return init(new UpdateLastAccessCommand(cacheName, key, segment, accessTime));
   }

   @Override
   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, int segment, Metadata metadata, long flagsBitSet) {
      return new ReplaceCommand(key, oldValue, newValue, metadata, segment, flagsBitSet, generateUUID(transactional));
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
   public SizeCommand buildSizeCommand(long flagsBitSet) {
      return new SizeCommand(flagsBitSet);
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
   public SingleRpcCommand buildSingleRpcCommand(ReplicableCommand call) {
      return new SingleRpcCommand(cacheName, call);
   }

   @Override
   public ClusteredGetCommand buildClusteredGetCommand(Object key, int segment, long flagsBitSet) {
      return new ClusteredGetCommand(key, cacheName, segment, flagsBitSet);
   }

   /**
    * @param isRemote true if the command is deserialized and is executed remote.
    */
   @Override
   public void initializeReplicableCommand(ReplicableCommand c, boolean isRemote) {
      if (c == null) return;

      if (c instanceof InitializableCommand) {
         ((InitializableCommand) c).init(componentRegistry, isRemote);
      } else {
         ModuleCommandInitializer mci = moduleCommandInitializers.get(c.getCommandId());
         if (mci != null)
            mci.initializeReplicableCommand(c, isRemote);
      }
   }

   @SuppressWarnings("unchecked")
   private <T> T init(InitializableCommand cmd) {
      cmd.init(componentRegistry, false);
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
   public StateRequestCommand buildStateRequestCommand(StateRequestCommand.Type subtype, Address sender, int topologyId, IntSet segments) {
      return new StateRequestCommand(cacheName, subtype, sender, topologyId, segments);
   }

   @Override
   public StateResponseCommand buildStateResponseCommand(Address sender, int topologyId, Collection<StateChunk> stateChunks, boolean applyState, boolean pushTransfer) {
      return new StateResponseCommand(cacheName, sender, topologyId, stateChunks, applyState, pushTransfer);
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
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(Xid xid, GlobalTransaction globalTransaction) {
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
   public CompleteTransactionCommand buildCompleteTransactionCommand(Xid xid, boolean commit) {
      return new CompleteTransactionCommand(cacheName, xid, commit);
   }

   @Override
   public CreateCacheCommand buildCreateCacheCommand(String cacheNameToCreate, String cacheConfigurationName) {
      return new CreateCacheCommand(cacheName, cacheNameToCreate, cacheConfigurationName);
   }

   @Override
   public CreateCacheCommand buildCreateCacheCommand(String cacheNameToCreate, String cacheConfigurationName, int size) {
      return new CreateCacheCommand(cacheName, cacheNameToCreate, cacheConfigurationName, size);
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
   public GetKeysInGroupCommand buildGetKeysInGroupCommand(long flagsBitSet, Object groupName) {
      return new GetKeysInGroupCommand(flagsBitSet, groupName);
   }

   @Override
   public <K> StreamRequestCommand<K> buildStreamRequestCommand(Object id, boolean parallelStream,
           StreamRequestCommand.Type type, IntSet segments, Set<K> keys, Set<K> excludedKeys,
           boolean includeLoader, boolean entryStream, Object terminalOperation) {
      return new StreamRequestCommand<>(cacheName, cacheManager.getAddress(), id, parallelStream, type,
              segments, keys, excludedKeys, includeLoader, entryStream, terminalOperation);
   }

   @Override
   public <R> StreamResponseCommand<R> buildStreamResponseCommand(Object identifier, boolean complete,
           IntSet lostSegments, R response) {
      return new StreamResponseCommand<>(cacheName, cacheManager.getAddress(), identifier, complete,
              lostSegments, response);
   }

   @Override
   public <K> StreamIteratorRequestCommand<K> buildStreamIteratorRequestCommand(Object id, boolean parallelStream,
         IntSet segments, Set<K> keys, Set<K> excludedKeys, boolean includeLoader, boolean entryStream,
         Iterable<IntermediateOperation> intOps, long batchSize) {
      return new StreamIteratorRequestCommand<>(cacheName, cacheManager.getAddress(), id, parallelStream,
            segments, keys, excludedKeys, includeLoader, entryStream, intOps, batchSize);
   }

   @Override
   public StreamIteratorNextCommand buildStreamIteratorNextCommand(Object id, long batchSize) {
      return new StreamIteratorNextCommand(cacheName, id, batchSize);
   }

   @Override
   public StreamIteratorCloseCommand buildStreamIteratorCloseCommand(Object id) {
      return new StreamIteratorCloseCommand(cacheName, cacheManager.getAddress(), id);
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
      return init(new ReadWriteKeyValueCommand<>(key, argument, f, segment, generateUUID(transactional), getValueMatcher(f),
            params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, R> ReadWriteKeyCommand<K, V, R> buildReadWriteKeyCommand(Object key,
         Function<ReadWriteEntryView<K, V>, R> f, int segment, Params params, DataConversion keyDataConversion,
         DataConversion valueDataConversion) {
      return init(new ReadWriteKeyCommand<>(key, f, segment, generateUUID(transactional), getValueMatcher(f), params, keyDataConversion, valueDataConversion));
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
   public InvalidateVersionsCommand buildInvalidateVersionsCommand(int topologyId, Object[] keys, int[] topologyIds, long[] versions, boolean removed) {
      return new InvalidateVersionsCommand(cacheName, topologyId, keys, topologyIds, versions, removed);
   }

   @Override
   public <K, V> WriteOnlyKeyCommand<K, V> buildWriteOnlyKeyCommand(
         Object key, Consumer<WriteEntryView<K, V>> f, int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new WriteOnlyKeyCommand<>(key, f, segment, generateUUID(transactional), getValueMatcher(f), params, keyDataConversion, valueDataConversion));
   }

   @Override
   public <K, V, T> WriteOnlyKeyValueCommand<K, V, T> buildWriteOnlyKeyValueCommand(Object key, Object argument, BiConsumer<T, WriteEntryView<K, V>> f,
         int segment, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return init(new WriteOnlyKeyValueCommand<>(key, argument, f, segment, generateUUID(transactional), getValueMatcher(f), params, keyDataConversion, valueDataConversion));
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
   public BackupAckCommand buildBackupAckCommand(long id, int topologyId) {
      return new BackupAckCommand(cacheName, id, topologyId);
   }

   @Override
   public BackupMultiKeyAckCommand buildBackupMultiKeyAckCommand(long id, int segment, int topologyId) {
      return new BackupMultiKeyAckCommand(cacheName, id, segment, topologyId);
   }

   @Override
   public ExceptionAckCommand buildExceptionAckCommand(long id, Throwable throwable, int topologyId) {
      return new ExceptionAckCommand(cacheName, id, throwable, topologyId);
   }

   private ValueMatcher getValueMatcher(Object o) {
      SerializeFunctionWith ann = o.getClass().getAnnotation(SerializeFunctionWith.class);
      if (ann != null)
         return ValueMatcher.valueOf(ann.valueMatcher().toString());

      Externalizer ext = ((GlobalMarshaller) marshaller).findExternalizerFor(o);
      if (ext instanceof LambdaExternalizer)
         return ValueMatcher.valueOf(((LambdaExternalizer) ext).valueMatcher(o).toString());

      return ValueMatcher.MATCH_ALWAYS;
   }

   @Override
   public RevokeBiasCommand buildRevokeBiasCommand(Address ackTarget, long id, int topologyId, Collection<Object> keys) {
      return new RevokeBiasCommand(cacheName, ackTarget, id, topologyId, keys);
   }

   @Override
   public RenewBiasCommand buildRenewBiasCommand(Object[] keys) {
      return new RenewBiasCommand(cacheName, keys);
   }

   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand() {
      return new SingleKeyBackupWriteCommand(cacheName);
   }

   @Override
   public SingleKeyFunctionalBackupWriteCommand buildSingleKeyFunctionalBackupWriteCommand() {
      return new SingleKeyFunctionalBackupWriteCommand(cacheName);
   }

   @Override
   public PutMapBackupWriteCommand buildPutMapBackupWriteCommand() {
      return new PutMapBackupWriteCommand(cacheName);
   }

   @Override
   public MultiEntriesFunctionalBackupWriteCommand buildMultiEntriesFunctionalBackupWriteCommand() {
      return new MultiEntriesFunctionalBackupWriteCommand(cacheName);
   }

   @Override
   public MultiKeyFunctionalBackupWriteCommand buildMultiKeyFunctionalBackupWriteCommand() {
      return new MultiKeyFunctionalBackupWriteCommand(cacheName);
   }

   @Override
   public <K, R> ReductionPublisherRequestCommand<K> buildKeyReductionPublisherCommand(boolean parallelStream,
         DeliveryGuarantee deliveryGuarantee, IntSet segments, Set<K> keys, Set<K> excludedKeys, boolean includeLoader,
         Function<? super Publisher<K>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return new ReductionPublisherRequestCommand<>(cacheName, parallelStream, deliveryGuarantee, segments, keys, excludedKeys,
            includeLoader, false, transformer, finalizer);
   }

   @Override
   public <K, V, R> ReductionPublisherRequestCommand<K> buildEntryReductionPublisherCommand(boolean parallelStream,
         DeliveryGuarantee deliveryGuarantee, IntSet segments, Set<K> keys, Set<K> excludedKeys, boolean includeLoader,
         Function<? super Publisher<CacheEntry<K, V>>, ? extends CompletionStage<R>> transformer,
         Function<? super Publisher<R>, ? extends CompletionStage<R>> finalizer) {
      return new ReductionPublisherRequestCommand<>(cacheName, parallelStream, deliveryGuarantee, segments, keys, excludedKeys,
            includeLoader, true, transformer, finalizer);
   }

   @Override
   public <K, I, R> InitialPublisherCommand<K, I, R> buildInitialPublisherCommand(Object requestId, DeliveryGuarantee deliveryGuarantee,
         int batchSize, IntSet segments, Set<K> keys, Set<K> excludedKeys, boolean includeLoader, boolean entryStream,
         boolean trackKeys, Function<? super Publisher<I>, ? extends Publisher<R>> transformer) {
      return init(new InitialPublisherCommand<>(cacheName, requestId, deliveryGuarantee, batchSize, segments, keys,
            excludedKeys, includeLoader, entryStream, trackKeys, transformer));
   }

   @Override
   public NextPublisherCommand buildNextPublisherCommand(Object requestId) {
      return new NextPublisherCommand(cacheName, requestId);
   }

   @Override
   public CancelPublisherCommand buildCancelPublisherCommand(Object requestId) {
      return new CancelPublisherCommand(cacheName, requestId);
   }

   @Override
   public <K, V> MultiClusterEventCommand<K, V> buildMultiClusterEventCommand(Map<UUID, Collection<ClusterEvent<K, V>>> events) {
      return new MultiClusterEventCommand<>(cacheName, events);
   }
}
