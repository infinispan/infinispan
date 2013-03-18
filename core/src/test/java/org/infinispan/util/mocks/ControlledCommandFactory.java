/*
 * JBoss, Home of Professional Open Source
 * Copyright 2012 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */

package org.infinispan.util.mocks;

import org.infinispan.Cache;
import org.infinispan.atomic.Delta;
import org.infinispan.commands.CancelCommand;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.CreateCacheCommand;
import org.infinispan.commands.ReplicableCommand;
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
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.ReclosableLatch;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.Xid;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

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
   public final List<ReplicableCommand> receivedCommands = new ArrayList<ReplicableCommand>();
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
               gate.await();
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
   public PutKeyValueCommand buildPutKeyValueCommand(Object key, Object value, long lifespanMillis, long maxIdleTimeMillis, Set<Flag> flags) {
      return actual.buildPutKeyValueCommand(key, value, lifespanMillis, maxIdleTimeMillis, flags);
   }

   @Override
   public VersionedPutKeyValueCommand buildVersionedPutKeyValueCommand(Object key, Object value, long lifespanMillis, long maxIdleTimeMillis, EntryVersion version, Set<Flag> flags) {
      return actual.buildVersionedPutKeyValueCommand(key, value, lifespanMillis, maxIdleTimeMillis, version, flags);
   }

   @Override
   public RemoveCommand buildRemoveCommand(Object key, Object value, Set<Flag> flags) {
      return actual.buildRemoveCommand(key, value, flags);
   }

   @Override
   public InvalidateCommand buildInvalidateCommand(Set<Flag> flags, Object... keys) {
      return actual.buildInvalidateCommand(flags, keys);
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(boolean forRehash, Set<Flag> flags, Collection<Object> keys) {
      return actual.buildInvalidateFromL1Command(forRehash, flags, keys);
   }

   @Override
   public InvalidateCommand buildInvalidateFromL1Command(Address origin, boolean forRehash, Set<Flag> flags, Collection<Object> keys) {
      return actual.buildInvalidateFromL1Command(origin, forRehash, flags, keys);
   }

   @Override
   public ReplaceCommand buildReplaceCommand(Object key, Object oldValue, Object newValue, long lifespanMillis, long maxIdleTimeMillis, Set<Flag> flags) {
      return actual.buildReplaceCommand(key, oldValue, newValue, lifespanMillis, maxIdleTimeMillis, flags);
   }

   @Override
   public SizeCommand buildSizeCommand() {
      return actual.buildSizeCommand();
   }

   @Override
   public GetKeyValueCommand buildGetKeyValueCommand(Object key, Set<Flag> flags) {
      return actual.buildGetKeyValueCommand(key, flags);
   }

   @Override
   public GetCacheEntryCommand buildGetCacheEntryCommand(Object key, Set<Flag> flags) {
      return actual.buildGetCacheEntryCommand(key, flags);
   }

   @Override
   public KeySetCommand buildKeySetCommand() {
      return actual.buildKeySetCommand();
   }

   @Override
   public ValuesCommand buildValuesCommand() {
      return actual.buildValuesCommand();
   }

   @Override
   public EntrySetCommand buildEntrySetCommand() {
      return actual.buildEntrySetCommand();
   }

   @Override
   public PutMapCommand buildPutMapCommand(Map<?, ?> map, long lifespanMillis, long maxIdleTimeMillis, Set<Flag> flags) {
      return actual.buildPutMapCommand(map, lifespanMillis, maxIdleTimeMillis, flags);
   }

   @Override
   public ClearCommand buildClearCommand(Set<Flag> flags) {
      return actual.buildClearCommand(flags);
   }

   @Override
   public EvictCommand buildEvictCommand(Object key, Set<Flag> flags) {
      return actual.buildEvictCommand(key, flags);
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
   public ClusteredGetCommand buildClusteredGetCommand(Object key, Set<Flag> flags, boolean acquireRemoteLock, GlobalTransaction gtx) {
      return actual.buildClusteredGetCommand(key, flags, acquireRemoteLock, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection<Object> keys, Set<Flag> flags, GlobalTransaction gtx) {
      return actual.buildLockControlCommand(keys, flags, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Object key, Set<Flag> flags, GlobalTransaction gtx) {
      return actual.buildLockControlCommand(key, flags, gtx);
   }

   @Override
   public LockControlCommand buildLockControlCommand(Collection<Object> keys, Set<Flag> flags) {
      return actual.buildLockControlCommand(keys, flags);
   }

   @Override
   public StateRequestCommand buildStateRequestCommand(StateRequestCommand.Type subtype, Address sender, int viewId, Set<Integer> segments) {
      return actual.buildStateRequestCommand(subtype, sender, viewId, segments);
   }

   @Override
   public StateResponseCommand buildStateResponseCommand(Address sender, int viewId, Collection<StateChunk> stateChunks) {
      return actual.buildStateResponseCommand(sender, viewId, stateChunks);
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
   public <KIn, VIn, KOut, VOut> MapCombineCommand<KIn, VIn, KOut, VOut> buildMapCombineCommand(String taskId, Mapper<KIn, VIn, KOut, VOut> m, Reducer<KOut, VOut> r, Collection<KIn> keys) {
      return actual.buildMapCombineCommand(taskId, m, r, keys);
   }

   @Override
   public <KOut, VOut> ReduceCommand<KOut, VOut> buildReduceCommand(String taskId, String destinationCache, Reducer<KOut, VOut> r, Collection<KOut> keys) {
      return actual.buildReduceCommand(taskId, destinationCache, r, keys);
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
   public CreateCacheCommand buildCreateCacheCommand(String tmpCacheName, String defaultTmpCacheConfigurationName, boolean start, int size) {
      return actual.buildCreateCacheCommand(tmpCacheName, defaultTmpCacheConfigurationName, start, size);
   }
}
