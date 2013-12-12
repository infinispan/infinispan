package org.infinispan.interceptors.distribution;

import org.infinispan.atomic.DeltaCompositeKey;
import org.infinispan.commands.write.ValueMatcher;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcOptions;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Handles the distribution of the transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class TxDistributionInterceptor extends BaseDistributionInterceptor {

   private static Log log = LogFactory.getLog(TxDistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private boolean isPessimisticCache;
   private boolean useClusteredWriteSkewCheck;

   private static final RecipientGenerator CLEAR_COMMAND_GENERATOR = new RecipientGenerator() {
      @Override
      public List<Address> generateRecipients() {
         return null;
      }

      @Override
      public Collection<Object> getKeys() {
         return InfinispanCollections.emptySet();
      }
   };

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         return handleTxWriteCommand(ctx, command, new SingleKeyRecipientGenerator(command.getKey()), false);
      } finally {
         if (ctx.isOriginLocal()) {
            // If the state transfer interceptor has to retry the command, it should ignore the previous value.
            command.setValueMatcher(command.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
         }
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         return handleTxWriteCommand(ctx, command, new SingleKeyRecipientGenerator(command.getKey()), false);
      } finally {
         if (ctx.isOriginLocal()) {
            // If the state transfer interceptor has to retry the command, it should ignore the previous value.
            command.setValueMatcher(command.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
         }
      }
   }

   @Start
   public void start() {
      isPessimisticCache = cacheConfiguration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
      useClusteredWriteSkewCheck = !isPessimisticCache &&
            cacheConfiguration.versioning().enabled() && cacheConfiguration.locking().writeSkewCheck();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (command.hasFlag(Flag.PUT_FOR_EXTERNAL_READ)) {
         return handleNonTxWriteCommand(ctx, command);
      }

      SingleKeyRecipientGenerator skrg = new SingleKeyRecipientGenerator(command.getKey());
      Object returnValue = handleTxWriteCommand(ctx, command, skrg, command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER));
      if (ctx.isOriginLocal()) {
         // If the state transfer interceptor has to retry the command, it should ignore the previous value.
         command.setValueMatcher(command.isSuccessful() ? ValueMatcher.MATCH_ALWAYS : ValueMatcher.MATCH_NEVER);
      }
      return returnValue;
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // don't bother with a remote get for the PutMapCommand!
      return handleTxWriteCommand(ctx, command, new MultipleKeysRecipientGenerator(command.getMap().keySet()), true);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleTxWriteCommand(ctx, command, CLEAR_COMMAND_GENERATOR, false);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return visitGetCommand(ctx, command, false);
   }

   private Object visitGetCommand(InvocationContext ctx, GetKeyValueCommand command,
         boolean isGetCacheEntry) throws Throwable {
      try {
         Object returnValue = invokeNextInterceptor(ctx, command);

         //if the cache entry has the value lock flag set, skip the remote get.
         CacheEntry entry = ctx.lookupEntry(command.getKey());
         boolean skipRemoteGet = entry != null && entry.skipRemoteGet();

         // need to check in the context as well since a null retval is not necessarily an indication of the entry not being
         // available.  It could just have been removed in the same tx beforehand.  Also don't bother with a remote get if
         // the entry is mapped to the local node.
         if (!skipRemoteGet && returnValue == null && ctx.isOriginLocal()) {
            Object key = command.getKey();
            if (needsRemoteGet(ctx, command)) {
               InternalCacheEntry ice = remoteGet(ctx, key, false, command);
               if (ice != null) {
                  returnValue = ice.getValue();
               }
            }
            if (returnValue == null && !ctx.isEntryRemovedInContext(command.getKey())) {
               returnValue = localGet(ctx, key, false, command, isGetCacheEntry);
            }
         }
         return returnValue;
      } catch (SuspectException e) {
         // retry
         return visitGetKeyValueCommand(ctx, command);
      }
   }

   protected void lockAndWrap(InvocationContext ctx, Object key, InternalCacheEntry ice, FlagAffectedCommand command) throws InterruptedException {
      boolean skipLocking = hasSkipLocking(command);
      long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
      lockManager.acquireLock(ctx, key, lockTimeout, skipLocking);
      entryFactory.wrapEntryForPut(ctx, key, ice, false, command, false);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         //In Pessimistic mode, the delta composite keys were sent to the wrong owner and never locked.
         ArrayList<Object> keyToCheckOwners = new ArrayList<Object>(command.getKeys().size());
         for (Object key : command.getKeys()) {
            if (key instanceof DeltaCompositeKey) {
               keyToCheckOwners.add(((DeltaCompositeKey) key).getDeltaAwareValueKey());
            } else {
               keyToCheckOwners.add(key);
            }
         }
         final Collection<Address> affectedNodes = cdl.getOwners(keyToCheckOwners);
         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(affectedNodes == null ? dm.getConsistentHash().getMembers() : affectedNodes);
         log.tracef("Registered remote locks acquired %s", affectedNodes);
         rpcManager.invokeRemotely(affectedNodes, command, rpcManager.getDefaultRpcOptions(true, false));
      }
      return invokeNextInterceptor(ctx, command);
   }

   // ---- TX boundary commands
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         sendCommitCommand(ctx, command);

      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);

      if (shouldInvokeRemoteTxCommand(ctx)) {
         boolean affectsAllNodes = ctx.getCacheTransaction().hasModification(ClearCommand.class);
         Collection<Address> recipients = affectsAllNodes ? dm.getWriteConsistentHash().getMembers() : cdl.getOwners(ctx.getAffectedKeys());
         recipients = recipients == null ? dm.getWriteConsistentHash().getMembers() : recipients;
         prepareOnAffectedNodes(ctx, command, recipients, defaultSynchronous);

         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(recipients == null ? dm.getWriteConsistentHash().getMembers() : recipients);
      }
      return retVal;
   }

   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command, Collection<Address> recipients, boolean sync) {
      try {
         // this method will return immediately if we're the only member (because exclude_self=true)
         RpcOptions rpcOptions;
         if (sync && command.isOnePhaseCommit()) {
            rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, false).build();
         } else {
            rpcOptions = rpcManager.getDefaultRpcOptions(sync);
         }
         rpcManager.invokeRemotely(recipients, command, rpcOptions);
      } finally {
         transactionRemotelyPrepared(ctx);
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         rpcManager.invokeRemotely(getCommitNodes(ctx), command, rpcManager.getDefaultRpcOptions(
               cacheConfiguration.transaction().syncRollbackPhase(), false));
      }

      return invokeNextInterceptor(ctx, command);
   }

   private Collection<Address> getCommitNodes(TxInvocationContext ctx) {
      LocalTransaction localTx = (LocalTransaction) ctx.getCacheTransaction();
      Collection<Address> affectedNodes = cdl.getOwners(ctx.getAffectedKeys());
      List<Address> members = dm.getConsistentHash().getMembers();
      return localTx.getCommitNodes(affectedNodes, rpcManager.getTopologyId(), members);
   }

   private void sendCommitCommand(TxInvocationContext ctx, CommitCommand command) throws TimeoutException, InterruptedException {
      Collection<Address> recipients = getCommitNodes(ctx);
      boolean syncCommitPhase = cacheConfiguration.transaction().syncCommitPhase();
      RpcOptions rpcOptions;
      if (syncCommitPhase) {
         rpcOptions = rpcManager.getRpcOptionsBuilder(ResponseMode.SYNCHRONOUS_IGNORE_LEAVERS, false  ).build();
      } else {
         rpcOptions = rpcManager.getDefaultRpcOptions(false, false);
      }
      rpcManager.invokeRemotely(recipients, command, rpcOptions);
   }

   private boolean shouldFetchRemoteValuesForWriteSkewCheck(InvocationContext ctx, WriteCommand cmd) {
      // TODO Dan: I don't think this method should always return true: the write skew check should only happen on the primary owner, and the primary owner always has the value already
      if (useClusteredWriteSkewCheck && ctx.isInTxScope() && dm.isRehashInProgress()) {
         for (Object key : cmd.getAffectedKeys()) {
            if (dm.isAffectedByRehash(key) && !dataContainer.containsKey(key)) return true;
         }
      }
      return false;
   }

   /**
    * If we are within one transaction we won't do any replication as replication would only be performed at commit
    * time. If the operation didn't originate locally we won't do any replication either.
    */
   private Object handleTxWriteCommand(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator, boolean skipRemoteGet) throws Throwable {
      // see if we need to load values from remote sources first
      if (ctx.isOriginLocal() && !skipRemoteGet || command.isConditional() || command.hasFlag(Flag.DELTA_WRITE)
            || shouldFetchRemoteValuesForWriteSkewCheck(ctx, command))
         remoteGetBeforeWrite(ctx, command, recipientGenerator);

      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to distribute.
      return invokeNextInterceptor(ctx, command);
   }

   private Object localGet(InvocationContext ctx, Object key, boolean isWrite,
         FlagAffectedCommand command, boolean isGetCacheEntry) throws Throwable {
      InternalCacheEntry ice = dataContainer.get(key);
      if (ice != null) {
         if (isWrite && isPessimisticCache && ctx.isInTxScope()) {
            ((TxInvocationContext) ctx).addAffectedKey(key);
         }
         if (!ctx.replaceValue(key, ice)) {
            if (isWrite)
               lockAndWrap(ctx, key, ice, command);
            else
               ctx.putLookedUpEntry(key, ice);
         }
         return isGetCacheEntry ? ice : ice.getValue();
      }
      return null;
   }

   protected void remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command, RecipientGenerator keygen) throws Throwable {
      // this should only happen if:
      //   a) unsafeUnreliableReturnValues is false
      //   b) unsafeUnreliableReturnValues is true, we are in a TX and the command is conditional or a delta write
      // On the backup owners, the value matching policy should be set to MATCH_ALWAYS, and command.isConditional() should return true
      if (isNeedReliableReturnValues(command) || command.isConditional() || command.hasFlag(Flag.DELTA_WRITE) ||
            shouldFetchRemoteValuesForWriteSkewCheck(ctx, command)) {
         for (Object k : keygen.getKeys()) {
            CacheEntry entry = ctx.lookupEntry(k);
            boolean skipRemoteGet =  entry != null && entry.skipRemoteGet();
            if (skipRemoteGet) {
               continue;
            }
            InternalCacheEntry ice = remoteGet(ctx, k, true, command);
            if (ice == null) {
               localGet(ctx, k, true, command, false);
            }
         }
      }
   }

   private InternalCacheEntry remoteGet(InvocationContext ctx, Object key, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      if (ctx.isOriginLocal() && !isValueAvailableLocally(dm.getReadConsistentHash(), key) || dm.isAffectedByRehash(key) && !dataContainer.containsKey(key)) {
         if (trace) log.tracef("Doing a remote get for key %s", key);

         boolean acquireRemoteLock = false;
         if (ctx.isInTxScope()) {
            TxInvocationContext txContext = (TxInvocationContext) ctx;
            acquireRemoteLock = isWrite && isPessimisticCache && !txContext.getAffectedKeys().contains(key);
         }
         // attempt a remote lookup
         InternalCacheEntry ice = retrieveFromRemoteSource(key, ctx, acquireRemoteLock, command, isWrite);

         if (acquireRemoteLock) {
            ((TxInvocationContext) ctx).addAffectedKey(key);
         }

         if (ice != null) {
            if (useClusteredWriteSkewCheck && ctx.isInTxScope()) {
               ((TxInvocationContext)ctx).getCacheTransaction().putLookedUpRemoteVersion(key, ice.getMetadata().version());
            }

            if (!ctx.replaceValue(key, ice)) {
               if (isWrite)
                  lockAndWrap(ctx, key, ice, command);
               else {
                  ctx.putLookedUpEntry(key, ice);
                  if (ctx.isInTxScope()) {
                     ((TxInvocationContext) ctx).getCacheTransaction().replaceVersionRead(key, ice.getMetadata().version());
                  }
               }
            }
            return ice;
         }
      } else {
         if (trace) log.tracef("Not doing a remote get for key %s since entry is mapped to current node (%s), or is in L1.  Owners are %s", key, rpcManager.getAddress(), dm.locate(key));
      }
      return null;
   }
}
