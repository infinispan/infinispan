package org.infinispan.util.mocks;

import static org.infinispan.xsite.XSiteAdminCommand.AdminOperation;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand.StateTransferControl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.transaction.xa.Xid;

import org.infinispan.Cache;
import org.infinispan.commands.CancelCommand;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.CreateCacheCommand;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
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
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.commands.write.BackupMultiKeyAckCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.ExceptionAckCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateVersionsCommand;
import org.infinispan.commands.write.PrimaryAckCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.IntSet;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.impl.Params;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
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
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.SingleXSiteRpcCommand;
import org.infinispan.xsite.XSiteAdminCommand;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

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
      ComponentRegistry componentRegistry = cache.getAdvancedCache().getComponentRegistry();
      final ControlledCommandFactory ccf = new ControlledCommandFactory(componentRegistry.getCommandsFactory(), toBlock);
      TestingUtil.replaceField(ccf, "commandsFactory", componentRegistry, ComponentRegistry.class);
      componentRegistry.registerComponent(ccf, CommandsFactory.class);

      //hack: re-add the component registry to the GlobalComponentRegistry's "namedComponents" (CHM) in order to correctly publish it for
      // when it will be read by the InboundInvocationHandlder. InboundInvocationHandlder reads the value from the GlobalComponentRegistry.namedComponents before using it
      componentRegistry.getGlobalComponentRegistry().registerNamedComponentRegistry(componentRegistry, EmbeddedCacheManager.DEFAULT_CACHE_NAME);
      return ccf;
   }

   @Override
   public PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, Metadata metadata, long flagsBitSet) {
      return actual.buildPutKeyValueCommand(key, value, metadata, flagsBitSet);
   }

   @Override
   public RemoveCommand buildRemoveCommand(Object key, Object value, long flagsBitSet) {
      return actual.buildRemoveCommand(key, value, flagsBitSet);
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
   public RemoveExpiredCommand buildRemoveExpiredCommand(Object key, Object value, Long lifespan) {
      return actual.buildRemoveExpiredCommand(key, value, lifespan);
   }

   @Override
   public RemoveExpiredCommand buildRemoveExpiredCommand(Object key, Object value) {
      return actual.buildRemoveExpiredCommand(key, value);
   }

   @Override
   public RetrieveLastAccessCommand buildRetrieveLastAccessCommand(Object key, Object value) {
      return actual.buildRetrieveLastAccessCommand(key, value);
   }

   @Override
   public UpdateLastAccessCommand buildUpdateLastAccessCommand(Object key, long accessTime) {
      return actual.buildUpdateLastAccessCommand(key, accessTime);
   }

   @Override
   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, Metadata metadata, long flagsBitSet) {
      return actual.buildReplaceCommand(key, oldValue, newValue, metadata, flagsBitSet);
   }

   @Override
   public ComputeCommand buildComputeCommand(Object key, BiFunction mappingFunction, boolean computeIfPresent, Metadata metadata, long flagsBitSet) {
      return actual.buildComputeCommand(key, mappingFunction, computeIfPresent, metadata, flagsBitSet);
   }

   @Override
   public ComputeIfAbsentCommand buildComputeIfAbsentCommand(Object key, Function mappingFunction, Metadata metadata, long flagsBitSet) {
      return actual.buildComputeIfAbsentCommand(key, mappingFunction, metadata, flagsBitSet);
   }

   @Override
   public SizeCommand buildSizeCommand(long flagsBitSet) {
      return actual.buildSizeCommand(flagsBitSet);
   }

   @Override
   public GetKeyValueCommand buildGetKeyValueCommand(Object key, long flagsBitSet) {
      return actual.buildGetKeyValueCommand(key, flagsBitSet);
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
   public EvictCommand buildEvictCommand(Object key, long flagsBitSet) {
      return actual.buildEvictCommand(key, flagsBitSet);
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
   public SingleRpcCommand buildSingleRpcCommand(ReplicableCommand call) {
      return actual.buildSingleRpcCommand(call);
   }

   @Override
   public ClusteredGetCommand buildClusteredGetCommand(Object key, int segment, long flagsBitSet) {
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
   public StateRequestCommand buildStateRequestCommand(StateRequestCommand.Type subtype, Address sender, int topologyId, Set<Integer> segments) {
      return actual.buildStateRequestCommand(subtype, sender, topologyId, segments);
   }

   @Override
   public StateResponseCommand buildStateResponseCommand(Address sender, int viewId, Collection<StateChunk> stateChunks, boolean applyState, boolean pushTransfer) {
      return actual.buildStateResponseCommand(sender, viewId, stateChunks, applyState, pushTransfer);
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
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(Xid xid, GlobalTransaction globalTransaction) {
      return actual.buildTxCompletionNotificationCommand(xid, globalTransaction);
   }

   @Override
   public <T> DistributedExecuteCommand<T> buildDistributedExecuteCommand(Callable<T> callable, Address sender, Collection keys) {
      return actual.buildDistributedExecuteCommand(callable, sender, keys);
   }

   @Override
   public GetInDoubtTxInfoCommand buildGetInDoubtTxInfoCommand() {
      return actual.buildGetInDoubtTxInfoCommand();
   }

   @Override
   public CompleteTransactionCommand buildCompleteTransactionCommand(Xid xid, boolean commit) {
      return actual.buildCompleteTransactionCommand(xid, commit);
   }

   @Override
   public TxCompletionNotificationCommand buildTxCompletionNotificationCommand(long internalId) {
      return actual.buildTxCompletionNotificationCommand(internalId);
   }

   @Override
   public CreateCacheCommand buildCreateCacheCommand(String cacheName, String cacheConfigurationName) {
      return actual.buildCreateCacheCommand(cacheName, cacheConfigurationName);
   }

   @Override
   public CancelCommand buildCancelCommandCommand(UUID commandUUID) {
      return actual.buildCancelCommandCommand(commandUUID);
   }

   @Override
   public CreateCacheCommand buildCreateCacheCommand(String tmpCacheName, String defaultTmpCacheConfigurationName, int size) {
      return actual.buildCreateCacheCommand(tmpCacheName, defaultTmpCacheConfigurationName, size);
   }

   @Override
   public XSiteStateTransferControlCommand buildXSiteStateTransferControlCommand(StateTransferControl control,
                                                                                 String siteName) {
      return actual.buildXSiteStateTransferControlCommand(control, siteName);
   }

   @Override
   public XSiteAdminCommand buildXSiteAdminCommand(String siteName, AdminOperation op, Integer afterFailures,
                                                   Long minTimeToWait) {
      return actual.buildXSiteAdminCommand(siteName, op, afterFailures, minTimeToWait);
   }

   @Override
   public XSiteStatePushCommand buildXSiteStatePushCommand(XSiteState[] chunk, long timeoutMillis) {
      return actual.buildXSiteStatePushCommand(chunk, timeoutMillis);
   }

   @Override
   public SingleXSiteRpcCommand buildSingleXSiteRpcCommand(VisitableCommand command) {
      return actual.buildSingleXSiteRpcCommand(command);
   }

   @Override
   public GetKeysInGroupCommand buildGetKeysInGroupCommand(long flagsBitSet, Object groupName) {
      return actual.buildGetKeysInGroupCommand(flagsBitSet, groupName);
   }

   @Override
   public <K> StreamRequestCommand<K> buildStreamRequestCommand(Object id, boolean parallelStream,
           StreamRequestCommand.Type type, Set<Integer> segments, Set<K> keys, Set<K> excludedKeys,
           boolean includeLoader, boolean entryStream, Object terminalOperation) {
      return actual.buildStreamRequestCommand(id, parallelStream, type, segments, keys, excludedKeys, includeLoader,
              entryStream, terminalOperation);
   }

   @Override
   public <R> StreamResponseCommand<R> buildStreamResponseCommand(Object identifier, boolean complete,
                                                                  Set<Integer> lostSegments, R response) {
      return actual.buildStreamResponseCommand(identifier, complete, lostSegments, response);
   }

   @Override
   public <K> StreamIteratorRequestCommand<K> buildStreamIteratorRequestCommand(Object id, boolean parallelStream,
         IntSet segments, Set<K> keys, Set<K> excludedKeys, boolean includeLoader, boolean entryStream,
         Iterable<IntermediateOperation> intOps, long batchSize) {
      return actual.buildStreamIteratorRequestCommand(id, parallelStream, segments, keys, excludedKeys, includeLoader,
            entryStream, intOps, batchSize);
   }

   @Override
   public StreamIteratorNextCommand buildStreamIteratorNextCommand(Object id, long batchSize) {
      return actual.buildStreamIteratorNextCommand(id, batchSize);
   }

   @Override
   public StreamIteratorCloseCommand buildStreamIteratorCloseCommand(Object id) {
      return actual.buildStreamIteratorCloseCommand(id);
   }

   @Override
   public GetCacheEntryCommand buildGetCacheEntryCommand(Object key, int segment, long flagsBitSet) {
      return actual.buildGetCacheEntryCommand(key, segment, flagsBitSet);
   }

   @Override
   public <K, V, R> ReadOnlyKeyCommand<K, V, R> buildReadOnlyKeyCommand(Object key, Function<EntryView.ReadEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildReadOnlyKeyCommand(key, f, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, R> ReadOnlyManyCommand<K, V, R> buildReadOnlyManyCommand(Collection<?> keys, Function<EntryView.ReadEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildReadOnlyManyCommand(keys, f, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, T, R> ReadWriteKeyValueCommand<K, V, T, R> buildReadWriteKeyValueCommand(Object key, Object argument, BiFunction<T, EntryView.ReadWriteEntryView<K, V>, R> f,
                                                                                          Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildReadWriteKeyValueCommand(key, argument, f, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, R> ReadWriteKeyCommand<K, V, R> buildReadWriteKeyCommand(
         Object key, Function<EntryView.ReadWriteEntryView<K, V>, R> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildReadWriteKeyCommand(key, f, params, keyDataConversion, valueDataConversion);
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
         Object key, Consumer<EntryView.WriteEntryView<K, V>> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildWriteOnlyKeyCommand(key, f, params, keyDataConversion, valueDataConversion);
   }

   @Override
   public <K, V, T> WriteOnlyKeyValueCommand<K, V, T> buildWriteOnlyKeyValueCommand(Object key, Object argument, BiConsumer<T, EntryView.WriteEntryView<K, V>> f, Params params, DataConversion keyDataConversion, DataConversion valueDataConversion) {
      return actual.buildWriteOnlyKeyValueCommand(key, argument, f, params, keyDataConversion, valueDataConversion);
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
   public BackupAckCommand buildBackupAckCommand(long id, int topologyId) {
      return actual.buildBackupAckCommand(id, topologyId);
   }

   @Override
   public BackupMultiKeyAckCommand buildBackupMultiKeyAckCommand(long id, int segment, int topologyId) {
      return actual.buildBackupMultiKeyAckCommand(id, segment, topologyId);
   }

   @Override
   public PrimaryAckCommand buildPrimaryAckCommand(long id, boolean success, Object value, Address[] waitFor) {
      return actual.buildPrimaryAckCommand(id, success, value, waitFor);
   }

   @Override
   public ExceptionAckCommand buildExceptionAckCommand(long id, Throwable throwable, int topologyId) {
      return actual.buildExceptionAckCommand(id, throwable, topologyId);
   }

   @Override
   public InvalidateVersionsCommand buildInvalidateVersionsCommand(int topologyId, Object[] keys, int[] topologyIds, long[] versions, boolean removed) {
      return actual.buildInvalidateVersionsCommand(topologyId, keys, topologyIds, versions, removed);
   }

   @Override
   public RevokeBiasCommand buildRevokeBiasCommand(Address ackTarget, long id, int topologyId, Collection<Object> keys) {
      return actual.buildRevokeBiasCommand(ackTarget, id, topologyId, keys);
   }

   @Override
   public RenewBiasCommand buildRenewBiasCommand(Object[] keys) {
      return actual.buildRenewBiasCommand(keys);
   }


   @Override
   public SingleKeyBackupWriteCommand buildSingleKeyBackupWriteCommand() {
      return actual.buildSingleKeyBackupWriteCommand();
   }

   @Override
   public SingleKeyFunctionalBackupWriteCommand buildSingleKeyFunctionalBackupWriteCommand() {
      return actual.buildSingleKeyFunctionalBackupWriteCommand();
   }

   @Override
   public PutMapBackupWriteCommand buildPutMapBackupWriteCommand() {
      return actual.buildPutMapBackupWriteCommand();
   }

   @Override
   public MultiEntriesFunctionalBackupWriteCommand buildMultiEntriesFunctionalBackupWriteCommand() {
      return actual.buildMultiEntriesFunctionalBackupWriteCommand();
   }

   @Override
   public MultiKeyFunctionalBackupWriteCommand buildMultiKeyFunctionalBackupWriteCommand() {
      return actual.buildMultiKeyFunctionalBackupWriteCommand();
   }
}
