package org.infinispan.util.mocks;

import org.infinispan.Cache;
import org.infinispan.atomic.Delta;
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
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.api.functional.EntryView;
import org.infinispan.context.Flag;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.functional.impl.Params;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.stream.impl.StreamRequestCommand;
import org.infinispan.stream.impl.StreamResponseCommand;
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

import javax.transaction.xa.Xid;
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

import static org.infinispan.xsite.XSiteAdminCommand.AdminOperation;
import static org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand.StateTransferControl;

/**
 * @author Mircea Markus
 * @since 5.2
 */
public class ControlledCommandFactory implements CommandsFactory {

   private static Log log = LogFactory.getLog(ControlledCommandFactory.class);

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
   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, Metadata metadata, long flagsBitSet) {
      return actual.buildReplaceCommand(key, oldValue, newValue, metadata, flagsBitSet);
   }

   @Override
   public SizeCommand buildSizeCommand(Set<Flag> flags) {
      return actual.buildSizeCommand(flags);
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
   public KeySetCommand buildKeySetCommand(Set<Flag> flags) {
      return actual.buildKeySetCommand(flags);
   }

   @Override
   public EntrySetCommand buildEntrySetCommand(Set<Flag> flags) {
      return actual.buildEntrySetCommand(flags);
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
   public MultipleRpcCommand buildReplicateCommand(List<ReplicableCommand> toReplicate) {
      return actual.buildReplicateCommand(toReplicate);
   }

   @Override
   public SingleRpcCommand buildSingleRpcCommand(ReplicableCommand call) {
      return actual.buildSingleRpcCommand(call);
   }

   @Override
   public ClusteredGetCommand buildClusteredGetCommand(Object key, long flagsBitSet, boolean acquireRemoteLock, GlobalTransaction gtx) {
      return actual.buildClusteredGetCommand(key, flagsBitSet, acquireRemoteLock, gtx);
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
   public StateRequestCommand buildStateRequestCommand(StateRequestCommand.Type subtype, Address sender, int viewId, Set<Integer> segments) {
      return actual.buildStateRequestCommand(subtype, sender, viewId, segments);
   }

   @Override
   public StateResponseCommand buildStateResponseCommand(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
      return actual.buildStateResponseCommand(sender, topologyId, stateChunks);
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
   public ApplyDeltaCommand buildApplyDeltaCommand(Object deltaAwareValueKey, Delta delta, Collection keys) {
      return actual.buildApplyDeltaCommand(deltaAwareValueKey, delta, keys);
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
   public GetKeysInGroupCommand buildGetKeysInGroupCommand(long flagsBitSet, String groupName) {
      return actual.buildGetKeysInGroupCommand(flagsBitSet, groupName);
   }

   @Override
   public <K> StreamRequestCommand<K> buildStreamRequestCommand(Object id, boolean parallelStream,
           StreamRequestCommand.Type type, Set<Integer> segments, Set<K> keys, Set<K> excludedKeys,
           boolean includeLoader, Object terminalOperation) {
      return actual.buildStreamRequestCommand(id, parallelStream, type, segments, keys, excludedKeys, includeLoader,
              terminalOperation);
   }

   @Override
   public <R> StreamResponseCommand<R> buildStreamResponseCommand(Object identifier, boolean complete,
                                                                  Set<Integer> lostSegments, R response) {
      return actual.buildStreamResponseCommand(identifier, complete, lostSegments, response);
   }

   @Override
   public GetCacheEntryCommand buildGetCacheEntryCommand(Object key, long flagsBitSet) {
      return actual.buildGetCacheEntryCommand(key, flagsBitSet);
   }

   @Override
   public <K, V, R> ReadOnlyKeyCommand<K, V, R> buildReadOnlyKeyCommand(K key, Function<EntryView.ReadEntryView<K, V>, R> f) {
      return actual.buildReadOnlyKeyCommand(key, f);
   }

   @Override
   public <K, V, R> ReadOnlyManyCommand<K, V, R> buildReadOnlyManyCommand(Set<? extends K> keys, Function<EntryView.ReadEntryView<K, V>, R> f) {
      return actual.buildReadOnlyManyCommand(keys, f);
   }

   @Override
   public <K, V> WriteOnlyKeyCommand<K, V> buildWriteOnlyKeyCommand(K key, Consumer<EntryView.WriteEntryView<V>> f, Params params) {
      return actual.buildWriteOnlyKeyCommand(key, f, params);
   }

   @Override
   public <K, V, R> ReadWriteKeyValueCommand<K, V, R> buildReadWriteKeyValueCommand(K key, V value, BiFunction<V, EntryView.ReadWriteEntryView<K, V>, R> f, Params params) {
      return actual.buildReadWriteKeyValueCommand(key, value, f, params);
   }

   @Override
   public <K, V, R> ReadWriteKeyCommand<K, V, R> buildReadWriteKeyCommand(K key, Function<EntryView.ReadWriteEntryView<K, V>, R> f, Params params) {
      return actual.buildReadWriteKeyCommand(key, f, params);
   }

   @Override
   public <K, V> WriteOnlyManyEntriesCommand<K, V> buildWriteOnlyManyEntriesCommand(
         Map<? extends K, ? extends V> entries, BiConsumer<V, EntryView.WriteEntryView<V>> f, Params params) {
      return actual.buildWriteOnlyManyEntriesCommand(entries, f, params);
   }

   @Override
   public <K, V> WriteOnlyKeyValueCommand<K, V> buildWriteOnlyKeyValueCommand(K key, V value, BiConsumer<V, EntryView.WriteEntryView<V>> f, Params params) {
      return actual.buildWriteOnlyKeyValueCommand(key, value, f, params);
   }

   @Override
   public <K, V> WriteOnlyManyCommand<K, V> buildWriteOnlyManyCommand(Set<? extends K> keys, Consumer<EntryView.WriteEntryView<V>> f, Params params) {
      return actual.buildWriteOnlyManyCommand(keys, f, params);
   }

   @Override
   public <K, V, R> ReadWriteManyCommand<K, V, R> buildReadWriteManyCommand(Set<? extends K> keys, Function<EntryView.ReadWriteEntryView<K, V>, R> f, Params params) {
      return actual.buildReadWriteManyCommand(keys, f, params);
   }

   @Override
   public <K, V, R> ReadWriteManyEntriesCommand<K, V, R> buildReadWriteManyEntriesCommand(Map<? extends K, ? extends V> entries, BiFunction<V, EntryView.ReadWriteEntryView<K, V>, R> f, Params params) {
      return actual.buildReadWriteManyEntriesCommand(entries, f, params);
   }

}
