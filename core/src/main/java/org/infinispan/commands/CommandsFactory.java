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

import org.infinispan.atomic.Delta;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.EntrySetCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.KeySetCommand;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
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
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.*;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.context.Flag;
import org.infinispan.distexec.mapreduce.Mapper;
import org.infinispan.distexec.mapreduce.Reducer;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.transaction.xa.Xid;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;

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
    * @param lifespanMillis lifespan in milliseconds.  -1 if lifespan is not used.
    * @param maxIdleTimeMillis max idle time in milliseconds.  -1 if maxIdle is not used.
    * @return a PutKeyValueCommand
    */
   PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, long lifespanMillis, long maxIdleTimeMillis, Set<Flag> flags);

   /**
    * Builds a special form of {@link PutKeyValueCommand} that also holds a reference to a version to be applied.
    * @param key key to put
    * @param value value to put
    * @param lifespanMillis lifespan in milliseconds.  -1 if lifespan is not used.
    * @param maxIdleTimeMillis max idle time in milliseconds.  -1 if maxIdle is not used.
    * @param version version to apply with this put
    * @return a PutKeyValueCommand
    */
   VersionedPutKeyValueCommand buildVersionedPutKeyValueCommand(Object key, Object value, long lifespanMillis, long maxIdleTimeMillis, EntryVersion version, Set<Flag> flags);

   /**
    * Builds a RemoveCommand
    * @param key key to remove
    * @param value value to check for ina  conditional remove, or null for an unconditional remove.
    * @return a RemoveCommand
    */
   RemoveCommand buildRemoveCommand(Object key, Object value, Set<Flag> flags);

   /**
    * Builds an InvalidateCommand
    * @param keys keys to invalidate
    * @return an InvalidateCommand
    */
   InvalidateCommand buildInvalidateCommand(Set<Flag> flags, Object... keys);

   /**
    * Builds an InvalidateFromL1Command
    * @param forRehash set to true if the invalidation is happening due to a new node taking ownership.  False if it is due to a write, changing the state of the entry.
    * @param keys keys to invalidate
    * @return an InvalidateFromL1Command
    */
   InvalidateCommand buildInvalidateFromL1Command(boolean forRehash, Set<Flag> flags, Collection<Object> keys);

   /**
    * @see #buildInvalidateFromL1Command(org.infinispan.remoting.transport.Address, boolean, java.util.Set, java.util.Collection)
    */
   InvalidateCommand buildInvalidateFromL1Command(Address origin, boolean forRehash, Set<Flag> flags, Collection<Object> keys);

   /**
    * Builds a ReplaceCommand
    * @param key key to replace
    * @param oldValue existing value to check for if conditional, null if unconditional.
    * @param newValue value to replace with
    * @param lifespanMillis lifespan in milliseconds.  -1 if lifespan is not used.
    * @param maxIdleTimeMillis max idle time in milliseconds.  -1 if maxIdle is not used.
    * @return a ReplaceCommand
    */
   ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, long lifespanMillis, long maxIdleTimeMillis, Set<Flag> flags);

   /**
    * Builds a SizeCommand
    * @return a SizeCommand
    */
   SizeCommand buildSizeCommand();

   /**
    * Builds a GetKeyValueCommand
    * @param key key to get
    * @return a GetKeyValueCommand
    */
   GetKeyValueCommand buildGetKeyValueCommand(Object key, Set<Flag> flags);

   /**
    * Builds a GetKeyValueCommand
    * @param key key to get
    * @return a GetKeyValueCommand
    */
   GetCacheEntryCommand buildGetCacheEntryCommand(Object key, Set<Flag> flags);

   /**
    * Builds a KeySetCommand
    * @return a KeySetCommand
    */
   KeySetCommand buildKeySetCommand();

   /**
    * Builds a ValuesCommand
    * @return a ValuesCommand
    */
   ValuesCommand buildValuesCommand();

   /**
    * Builds a EntrySetCommand
    * @return a EntrySetCommand
    */
   EntrySetCommand buildEntrySetCommand();

   /**
    * Builds a PutMapCommand
    * @param map map containing key/value entries to put
    * @param lifespanMillis lifespan in milliseconds.  -1 if lifespan is not used.
    * @param maxIdleTimeMillis max idle time in milliseconds.  -1 if maxIdle is not used.
    * @return a PutMapCommand
    */
   PutMapCommand buildPutMapCommand(Map<?, ?> map, long lifespanMillis, long maxIdleTimeMillis, Set<Flag> flags);

   /**
    * Builds a ClearCommand
    * @return a ClearCommand
    */
   ClearCommand buildClearCommand(Set<Flag> flags);

   /**
    * Builds an EvictCommand
    * @param key key to evict
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
    * Builds a LockControlCommand to control explicit remote locking
    *
    *
    * @param keys keys to lock
    * @param gtx
    * @return a LockControlCommand
    */
   LockControlCommand buildLockControlCommand(Collection<Object> keys, Set<Flag> flags, GlobalTransaction gtx);

   /**
    * Same as {@link #buildLockControlCommand(Object, java.util.Set, org.infinispan.transaction.xa.GlobalTransaction)}
    * but for locking a single key vs a collection of keys.
    */
   LockControlCommand buildLockControlCommand(Object key, Set<Flag> flags, GlobalTransaction gtx);


   LockControlCommand buildLockControlCommand(Collection<Object> keys, Set<Flag> flags);

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
    * Same as {@link #buildCreateCacheCommand(String, String, false)}.
    */
   CreateCacheCommand buildCreateCacheCommand(String cacheName, String cacheConfigurationName);

   /**
    * Builds a CreateCacheCommand used to create/start cache around Infinispan cluster
    *
    * @param start if true, then this command also makes sure that the cache is started on all the nodes in the cluster.
    * @param size the expected number of nodes where this node runs
    */
   CreateCacheCommand buildCreateCacheCommand(String tmpCacheName, String defaultTmpCacheConfigurationName, boolean start, int size);

   /**
    * Builds CancelCommandCommand used to cancel other commands executing on Infinispan cluster
    * 
    * @param commandUUID UUID for command to cancel
    * @return created CancelCommandCommand
    */
   CancelCommand buildCancelCommandCommand(UUID commandUUID);
}
