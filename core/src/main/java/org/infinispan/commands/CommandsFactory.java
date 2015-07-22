package org.infinispan.commands;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.iteration.impl.EntryRequestCommand;
import org.infinispan.iteration.impl.EntryResponseCommand;
import org.infinispan.metadata.Metadata;
import org.infinispan.atomic.Delta;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commands.read.SizeCommand;
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
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.*;
import org.infinispan.commons.CacheException;
import org.infinispan.context.Flag;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.remoting.transport.Address;
import org.infinispan.stream.impl.StreamRequestCommand;
import org.infinispan.stream.impl.StreamResponseCommand;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.xsite.SingleXSiteRpcCommand;
import org.infinispan.xsite.XSiteAdminCommand;
import org.infinispan.xsite.statetransfer.XSiteState;
import org.infinispan.xsite.statetransfer.XSiteStatePushCommand;
import org.infinispan.xsite.statetransfer.XSiteStateTransferControlCommand;

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
 * A factory to build commands, initializing and injecting dependencies accordingly.  Commands built for a specific,
 * named cache instance cannot be reused on a different cache instance since most commands contain the cache name it
 * was built for along with references to other named-cache scoped components.
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Galder Zamarreño
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface CommandsFactory {

   /**
    * Builds a PutKeyValueCommand
    * @param key key to put
    * @param value value to put
    * @param metadata metadata of entry
    * @param flags Command flags provided by cache
    * @return a PutKeyValueCommand
    */
   PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, Metadata metadata, Set<Flag> flags);

   /**
    * Builds a RemoveCommand
    * @param key key to remove
    * @param value value to check for ina  conditional remove, or null for an unconditional remove.
    * @param flags Command flags provided by cache
    * @return a RemoveCommand
    */
   RemoveCommand buildRemoveCommand(Object key, Object value, Set<Flag> flags);

   /**
    * Builds an InvalidateCommand
    * @param flags Command flags provided by cache
    * @param keys keys to invalidate
    * @return an InvalidateCommand
    */
   InvalidateCommand buildInvalidateCommand(Set<Flag> flags, Object... keys);

   /**
    * Builds an InvalidateFromL1Command
    * @param keys keys to invalidate
    * @return an InvalidateFromL1Command
    */
   InvalidateCommand buildInvalidateFromL1Command(Set<Flag> flags, Collection<Object> keys);

   /**
    * @see #buildInvalidateFromL1Command(java.util.Set, java.util.Collection)
    */
   InvalidateCommand buildInvalidateFromL1Command(Address origin, Set<Flag> flags, Collection<Object> keys);

   /**
    * Builds a ReplaceCommand
    * @param key key to replace
    * @param oldValue existing value to check for if conditional, null if unconditional.
    * @param newValue value to replace with
    * @param metadata metadata of entry
    * @param flags Command flags provided by cache
    * @return a ReplaceCommand
    */
   ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, Metadata metadata, Set<Flag> flags);

   /**
    * Builds a SizeCommand
    * @param flags Command flags provided by cache
    * @return a SizeCommand
    */
   SizeCommand buildSizeCommand(Set<Flag> flags);

   /**
    * Builds a GetKeyValueCommand
    * @param key key to get
    * @param flags Command flags provided by cache
    * @return a GetKeyValueCommand
    */
   GetKeyValueCommand buildGetKeyValueCommand(Object key, Set<Flag> flags);

   /**
    * Builds a GetCacheEntryCommand
    * @param key key to get
    * @param explicitFlags Command flags provided by cache
    * @return a GetCacheEntryCommand
    */
   GetCacheEntryCommand buildGetCacheEntryCommand(Object key, Set<Flag> explicitFlags);

   /**
    * Builds a GetAllCommand
    * @param keys keys to get
    * @param flags Command flags provided by cache
    * @param returnEntries boolean indicating whether entire cache entries are
    *                      returned, otherwise return just the value parts
    * @return a GetKeyValueCommand
    */
   GetAllCommand buildGetAllCommand(Collection<?> keys, Set<Flag> flags, boolean returnEntries);

   /**
    * Builds a KeySetCommand
    * @param flags Command flags provided by cache
    * @return a KeySetCommand
    */
   KeySetCommand buildKeySetCommand(Set<Flag> flags);

   /**
    * Builds a EntrySetCommand
    * @param flags Command flags provided by cache
    * @return a EntrySetCommand
    */
   EntrySetCommand buildEntrySetCommand(Set<Flag> flags);

   /**
    * Builds a PutMapCommand
    * @param map map containing key/value entries to put
    * @param metadata metadata of entry
    * @param flags Command flags provided by cache
    * @return a PutMapCommand
    */
   PutMapCommand buildPutMapCommand(Map<?, ?> map, Metadata metadata, Set<Flag> flags);

   /**
    * Builds a ClearCommand
    * @param flags Command flags provided by cache
    * @return a ClearCommand
    */
   ClearCommand buildClearCommand(Set<Flag> flags);

   /**
    * Builds an EvictCommand
    * @param key key to evict
    * @param flags Command flags provided by cache
    * @return an EvictCommand
    */
   EvictCommand buildEvictCommand(Object key, Set<Flag> flags);

   /**
    * Builds a PrepareCommand
    * @param gtx global transaction associated with the prepare
    * @param modifications list of modifications
    * @param onePhaseCommit is this a one-phase or two-phase transaction?
    * @return a PrepareCommand
    */
   PrepareCommand buildPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhaseCommit);

   /**
    * Builds a VersionedPrepareCommand
    *
    * @param gtx global transaction associated with the prepare
    * @param modifications list of modifications
    * @param onePhase
    * @return a VersionedPrepareCommand
    */
   VersionedPrepareCommand buildVersionedPrepareCommand(GlobalTransaction gtx, List<WriteCommand> modifications, boolean onePhase);

   /**
    * Builds a CommitCommand
    * @param gtx global transaction associated with the commit
    * @return a CommitCommand
    */
   CommitCommand buildCommitCommand(GlobalTransaction gtx);

   /**
    * Builds a VersionedCommitCommand
    * @param gtx global transaction associated with the commit
    * @return a VersionedCommitCommand
    */
   VersionedCommitCommand buildVersionedCommitCommand(GlobalTransaction gtx);

   /**
    * Builds a RollbackCommand
    * @param gtx global transaction associated with the rollback
    * @return a RollbackCommand
    */
   RollbackCommand buildRollbackCommand(GlobalTransaction gtx);

   /**
    * Initializes a {@link org.infinispan.commands.ReplicableCommand} read from a data stream with components specific
    * to the target cache instance.
    * <p/>
    * Implementations should also be deep, in that if the command contains other commands, these should be recursed
    * into.
    * <p/>
    *
    * @param command command to initialize.  Cannot be null.
    * @param isRemote
    */
   void initializeReplicableCommand(ReplicableCommand command, boolean isRemote);

   /**
    * Builds an RpcCommand "envelope" containing multiple ReplicableCommands
    * @param toReplicate ReplicableCommands to include in the envelope
    * @return a MultipleRpcCommand
    */
   MultipleRpcCommand buildReplicateCommand(List<ReplicableCommand> toReplicate);

   /**
    * Builds a SingleRpcCommand "envelope" containing a single ReplicableCommand
    * @param call ReplicableCommand to include in the envelope
    * @return a SingleRpcCommand
    */
   SingleRpcCommand buildSingleRpcCommand(ReplicableCommand call);

   /**
    * Builds a ClusteredGetCommand, which is a remote lookup command
    * @param key key to look up
    * @return a ClusteredGetCommand
    */
   ClusteredGetCommand buildClusteredGetCommand(Object key, Set<Flag> flags, boolean acquireRemoteLock, GlobalTransaction gtx);

   /**
    * Builds a ClusteredGetAllCommand, which is a remote lookup command
    * @param keys key to look up
    * @return a ClusteredGetAllCommand
    */
   ClusteredGetAllCommand buildClusteredGetAllCommand(List<?> keys, Set<Flag> flags, GlobalTransaction gtx);

   /**
    * Builds a LockControlCommand to control explicit remote locking
    *
    *
    * @param keys keys to lock
    * @param gtx
    * @return a LockControlCommand
    */
   LockControlCommand buildLockControlCommand(Collection<?> keys, Set<Flag> flags, GlobalTransaction gtx);

   /**
    * Same as {@link #buildLockControlCommand(Object, java.util.Set, org.infinispan.transaction.xa.GlobalTransaction)}
    * but for locking a single key vs a collection of keys.
    */
   LockControlCommand buildLockControlCommand(Object key, Set<Flag> flags, GlobalTransaction gtx);


   LockControlCommand buildLockControlCommand(Collection<?> keys, Set<Flag> flags);

   /**
    * Builds a StateRequestCommand used for requesting transactions and locks and for starting or canceling transfer of cache entries.
    */
   StateRequestCommand buildStateRequestCommand(StateRequestCommand.Type subtype, Address sender, int viewId, Set<Integer> segments);

   /**
    * Builds a StateResponseCommand used for pushing cache entries to another node in response to a StateRequestCommand.
    */
   StateResponseCommand buildStateResponseCommand(Address sender, int viewId, Collection<StateChunk> stateChunks);

   /**
    * Retrieves the cache name this CommandFactory is set up to construct commands for.
    * @return the name of the cache this CommandFactory is set up to construct commands for.
    */
   String getCacheName();

   /**
    * Builds a {@link org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand}.
    */
   GetInDoubtTransactionsCommand buildGetInDoubtTransactionsCommand();

   /**
    * Builds a {@link org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand}.
    */
   TxCompletionNotificationCommand buildTxCompletionNotificationCommand(Xid xid, GlobalTransaction globalTransaction);

   /**
    * Builds a DistributedExecuteCommand used for migration and execution of distributed Callables and Runnables.
    *
    * @param callable the callable task
    * @param sender sender's Address
    * @param keys keys used in Callable
    * @return a DistributedExecuteCommand
    */
   <T>DistributedExecuteCommand<T> buildDistributedExecuteCommand(Callable<T> callable, Address sender, Collection keys);

   /**
    * Builds a MapCombineCommand used for migration and map phase execution of MapReduce tasks.
    *
    * @param m Mapper for MapReduceTask
    * @param r Combiner for MapReduceTask
    * @param keys keys used in MapReduceTask
    * @return created MapCombineCommand
    */
   <KIn, VIn, KOut, VOut> MapCombineCommand<KIn, VIn, KOut, VOut> buildMapCombineCommand(
            String taskId, Mapper<KIn, VIn, KOut, VOut> m, Reducer<KOut, VOut> r,
            Collection<KIn> keys);

   /**
    * Builds a ReduceCommand used for migration and reduce phase execution of MapReduce tasks.
    *
    * @param r Reducer for MapReduceTask
    * @param keys keys used in MapReduceTask
    * @return created ReduceCommand
    */
   <KOut, VOut> ReduceCommand<KOut, VOut> buildReduceCommand(String taskId,
            String destinationCache, Reducer<KOut, VOut> r, Collection<KOut> keys);

   /**
    * @see GetInDoubtTxInfoCommand
    */
   GetInDoubtTxInfoCommand buildGetInDoubtTxInfoCommand();

   /**
    * Builds a CompleteTransactionCommand command.
    * @param xid the xid identifying the transaction we want to complete.
    * @param commit commit(true) or rollback(false)?
    */
   CompleteTransactionCommand buildCompleteTransactionCommand(Xid xid, boolean commit);

   /**
    * @param internalId the internal id identifying the transaction to be removed.
    * @see org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand
    */
   TxCompletionNotificationCommand buildTxCompletionNotificationCommand(long internalId);


   /**
    * Builds a ApplyDeltaCommand used for applying Delta objects to DeltaAware containers stored in cache
    *
    * @return ApplyDeltaCommand instance
    * @see ApplyDeltaCommand
    */
   ApplyDeltaCommand buildApplyDeltaCommand(Object deltaAwareValueKey, Delta delta, Collection keys);

   /**
    * Same as {@code buildCreateCacheCommand(cacheName, cacheConfigurationName, false, 0)}.
    */
   CreateCacheCommand buildCreateCacheCommand(String cacheName, String cacheConfigurationName);

   /**
    * Builds a CreateCacheCommand used to create/start cache around Infinispan cluster
    *
    * @param size If {@code size > 0}, the command will wait until the cache runs on at least {@code size} nodes.
    */
   CreateCacheCommand buildCreateCacheCommand(String tmpCacheName, String defaultTmpCacheConfigurationName, int size);

   /**
    * Builds CancelCommandCommand used to cancel other commands executing on Infinispan cluster
    *
    * @param commandUUID UUID for command to cancel
    * @return created CancelCommandCommand
    */
   CancelCommand buildCancelCommandCommand(UUID commandUUID);

   /**
    * Builds XSiteStateTransferControlCommand used to control the-cross site state transfer.
    *
    * @param control  the control operation
    * @param siteName the site name, needed for some control operations.
    * @return the XSiteStateTransferControlCommand created
    */
   XSiteStateTransferControlCommand buildXSiteStateTransferControlCommand(StateTransferControl control, String siteName);

   /**
    * Builds XSiteAdminCommand used to perform system administrator operations.
    *
    * @return the XSiteAdminCommand created
    */
   XSiteAdminCommand buildXSiteAdminCommand(String siteName, AdminOperation op, Integer afterFailures, Long minTimeToWait);

   /**
    * Builds XSiteStatePushCommand used to transfer a single chunk of data between sites.
    *
    * @param chunk         the data chunk
    * @param timeoutMillis timeout in milliseconds, for the retries in the receiver site.
    * @return the XSiteStatePushCommand created
    */
   XSiteStatePushCommand buildXSiteStatePushCommand(XSiteState[] chunk, long timeoutMillis);

   /**
    * Builds SingleRpcCommand used to perform {@link org.infinispan.commands.VisitableCommand} on the backup site,
    * @param command the visitable command.
    * @return the SingleXSiteRpcCommand created
    */
   SingleXSiteRpcCommand buildSingleXSiteRpcCommand(VisitableCommand command);

   /**
    * Builds {@link org.infinispan.iteration.impl.EntryRequestCommand} used to request entries from a remote node for
    * given segments
    * @param identifier The unique identifier for this entry retrieval request
    * @param segments The segments this request should retrieve
    * @param filter The filter to apply to any found values to limit response data
    * @param converter The converter to apply to any found values
    * @param flags The flags used to modify behavior
    * @param <K> The key type of the stored key
    * @param <V> The value type of the stored values
    * @param <C> The converted type after the value is applied from the converter
    * @return the EntryRequestCommand created
    */
   <K, V, C> EntryRequestCommand<K, V, C> buildEntryRequestCommand(UUID identifier, Set<Integer> segments, Set<K> keysToFilter,
                                                KeyValueFilter<? super K, ? super V> filter,
                                                Converter<? super K, ? super V, C> converter, Set<Flag> flags);

   /**
    * Builds {@link org.infinispan.iteration.impl.EntryResponseCommand} use to respond with retrieved entries for
    * given segments
    * @param identifier The unique identifier for this entry retrieval request
    * @param completedSegments The segments that are now completed per this response
    * @param inDoubtSegments The segements that are now in doubt meaning they must be retrieved again from another
    *                        node due to rehash
    * @param values The entries retrieved from the remote node
    * @param e If an exception occurred while running the processing on the remote node
    * @param <K> The key type of the stored key
    * @param <C> The converted type after the value is applied from the converter
    * @return The EntryResponseCommand created
    */
   <K, C> EntryResponseCommand<K, C> buildEntryResponseCommand(UUID identifier, Set<Integer> completedSegments,
                                                         Set<Integer> inDoubtSegments, Collection<CacheEntry<K, C>> values,
                                                         CacheException e);

   /**
    * Builds {@link org.infinispan.commands.remote.GetKeysInGroupCommand} used to fetch all the keys belonging to a group.
    *
    * @param flags
    * @param groupName the group name.
    * @return the GetKeysInGroup created.
    */
   GetKeysInGroupCommand buildGetKeysInGroupCommand(Set<Flag> flags, String groupName);

   <K> StreamRequestCommand<K> buildStreamRequestCommand(UUID id, boolean parallelStream, StreamRequestCommand.Type type,
           Set<Integer> segments, Set<K> keys, Set<K> excludedKeys, boolean includeLoader, Object terminalOperation);

   /**
    * Builds {@link StreamResponseCommand} used to send back a response either intermediate or complete to the
    * originating node with the information for the stream request.
    * @param identifier the unique identifier for the stream request
    * @param complete whether or not this is an intermediate or final response from this node for the given id
    * @param lostSegments what segments that were lost during processing
    * @param response the actual response
    * @param <R> type of response
    * @return the command to send back the response
    */
   <R> StreamResponseCommand<R> buildStreamResponseCommand(UUID identifier, boolean complete, Set<Integer> lostSegments,
           R response);
}
