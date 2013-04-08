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

package org.infinispan.interceptors.distribution;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.L1Manager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Future;
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

   private L1Manager l1Manager;
   private boolean isL1CacheEnabled;

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

   @Inject
   public void init (L1Manager l1Manager) {
      this.l1Manager = l1Manager;
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      try {
         return super.visitReplaceCommand(ctx, command);
      } finally {
         boolean ignorePreviousValues = ignorePreviousValueOnBackup(command, ctx);
         command.setIgnorePreviousValue(ignorePreviousValues);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      try {
         return super.visitRemoveCommand(ctx, command);
      } finally {
         boolean ignorePreviousValues = ignorePreviousValueOnBackup(command, ctx);
         command.setIgnorePreviousValue(ignorePreviousValues);
      }
   }

   /**
    * For conditional operations (replace, remove, put if absent) Used only for optimistic transactional caches, to solve the following situation:
    * <pre>
    * - node A (owner, tx originator) does a successful replace
    * - the actual value changes
    * - tx commits. The value is applied on A (the check was performed at operation time) but is not applied on
    *   B (check is performed at commit time).
    * In such situations (optimistic caches) the remote conditional command should not re-check the old value.
    * </pre>
    */
   protected boolean ignorePreviousValueOnBackup(WriteCommand command, InvocationContext ctx) {
      return super.ignorePreviousValueOnBackup(command, ctx)
          && cacheConfiguration.transaction().lockingMode() == LockingMode.OPTIMISTIC && !useClusteredWriteSkewCheck;
   }

   @Start
   public void start() {
      isPessimisticCache = cacheConfiguration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
      isL1CacheEnabled = cacheConfiguration.clustering().l1().enabled();
      useClusteredWriteSkewCheck = !isPessimisticCache &&
            cacheConfiguration.versioning().enabled() && cacheConfiguration.locking().writeSkewCheck();
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      SingleKeyRecipientGenerator skrg = new SingleKeyRecipientGenerator(command.getKey());
      Object returnValue = handleWriteCommand(ctx, command, skrg, command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER), false);
      if (ignorePreviousValueOnBackup(command, ctx)) {
         command.setPutIfAbsent(false);
      }
      // If this was a remote put record that which sent it
      if (isL1CacheEnabled && !ctx.isOriginLocal() && !skrg.generateRecipients().contains(ctx.getOrigin()))
         l1Manager.addRequestor(command.getKey(), ctx.getOrigin());

      return returnValue;
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleWriteCommand(ctx, command, CLEAR_COMMAND_GENERATOR, false, true);
   }


   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      try {
         Object returnValue = invokeNextInterceptor(ctx, command);
         // If L1 caching is enabled, this is a remote command, and we found a value in our cache
         // we store it so that we can later invalidate it
         if (returnValue != null && isL1CacheEnabled && !ctx.isOriginLocal()) l1Manager.addRequestor(command.getKey(), ctx.getOrigin());

         // need to check in the context as well since a null retval is not necessarily an indication of the entry not being
         // available.  It could just have been removed in the same tx beforehand.  Also don't bother with a remote get if
         // the entry is mapped to the local node.
         if (returnValue == null) {
            Object key = command.getKey();
            if (needsRemoteGet(ctx, command)) {
               returnValue = remoteGetAndStoreInL1(ctx, key, false, command);
            }
            if (returnValue == null) {
               returnValue = localGet(ctx, key, false, command);
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
      entryFactory.wrapEntryForPut(ctx, key, ice, false, command);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         final Collection<Address> affectedNodes = dm.getAffectedNodes(command.getKeys());
         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(affectedNodes);
         log.tracef("Registered remote locks acquired %s", affectedNodes);
         rpcManager.invokeRemotely(affectedNodes, command, rpcManager.getDefaultRpcOptions(true, false));
      }
      return invokeNextInterceptor(ctx, command);
   }

   // ---- TX boundary commands
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Future<?> f = flushL1Caches(ctx);
         sendCommitCommand(ctx, command);
         blockOnL1FutureIfNeeded(f);

      } else if (isL1CacheEnabled && !ctx.isOriginLocal() && !ctx.getLockedKeys().isEmpty()) {
         // We fall into this block if we are a remote node, happen to be the primary data owner and have locked keys.
         // it is still our responsibility to invalidate L1 caches in the cluster.
         blockOnL1FutureIfNeeded(flushL1Caches(ctx));
      }
      return invokeNextInterceptor(ctx, command);
   }

   private void blockOnL1FutureIfNeeded(Future<?> f) {
      if (f != null && cacheConfiguration.transaction().syncCommitPhase()) {
         try {
            f.get();
         } catch (Exception e) {
            // Ignore SuspectExceptions - if the node has gone away then there is nothing to invalidate anyway.
            if (!(e.getCause() instanceof SuspectException)) {
               getLog().failedInvalidatingRemoteCache(e);
            }
         }
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);

      if (shouldInvokeRemoteTxCommand(ctx)) {
         if (command.isOnePhaseCommit()) flushL1Caches(ctx); // if we are one-phase, don't block on this future.

         boolean affectsAllNodes = ctx.getCacheTransaction().hasModification(ClearCommand.class);
         Collection<Address> recipients = affectsAllNodes ? dm.getWriteConsistentHash().getMembers() : dm.getAffectedNodes(ctx.getAffectedKeys());
         prepareOnAffectedNodes(ctx, command, recipients, defaultSynchronous);

         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(recipients);
      } else if (isL1CacheEnabled && command.isOnePhaseCommit() && !ctx.isOriginLocal() && !ctx.getLockedKeys().isEmpty()) {
         // We fall into this block if we are a remote node, happen to be the primary data owner and have locked keys.
         // it is still our responsibility to invalidate L1 caches in the cluster.
         flushL1Caches(ctx);
      }
      return retVal;
   }

   protected void prepareOnAffectedNodes(TxInvocationContext ctx, PrepareCommand command, Collection<Address> recipients, boolean sync) {
      // this method will return immediately if we're the only member (because exclude_self=true)
      rpcManager.invokeRemotely(recipients, command, rpcManager.getDefaultRpcOptions(sync));
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
      Collection<Address> affectedNodes = dm.getAffectedNodes(ctx.getAffectedKeys());
      List<Address> members = dm.getConsistentHash().getMembers();
      return localTx.getCommitNodes(affectedNodes, rpcManager.getTopologyId(), members);
   }

   protected void sendCommitCommand(TxInvocationContext ctx, CommitCommand command) throws TimeoutException, InterruptedException {
      Collection<Address> recipients = getCommitNodes(ctx);
      boolean syncCommitPhase = cacheConfiguration.transaction().syncCommitPhase();
      rpcManager.invokeRemotely(recipients, command, rpcManager.getDefaultRpcOptions(syncCommitPhase, false));
   }

   private boolean shouldFetchRemoteValuesForWriteSkewCheck(InvocationContext ctx, WriteCommand cmd) {
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
   protected Object handleWriteCommand(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator, boolean skipRemoteGet, boolean skipL1Invalidation) throws Throwable {
      // see if we need to load values from remote sources first
      if (ctx.isOriginLocal() && !skipRemoteGet || command.isConditional() || shouldFetchRemoteValuesForWriteSkewCheck(ctx, command))
         remoteGetBeforeWrite(ctx, command, recipientGenerator);

      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to distribute.
      return invokeNextInterceptor(ctx, command);
   }

   private Object localGet(InvocationContext ctx, Object key, boolean isWrite, FlagAffectedCommand command) throws Throwable {
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
         return command instanceof GetCacheEntryCommand ? ice : ice.getValue();
      }
      return null;
   }

   private void remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command, KeyGenerator keygen) throws Throwable {
      // this should only happen if:
      //   a) unsafeUnreliableReturnValues is false
      //   b) unsafeUnreliableReturnValues is true, we are in a TX and the command is conditional
      if (isNeedReliableReturnValues(command) || command.isConditional() || shouldFetchRemoteValuesForWriteSkewCheck(ctx, command)) {
         for (Object k : keygen.getKeys()) {
            Object returnValue = remoteGetAndStoreInL1(ctx, k, true, command);
            if (returnValue == null) {
               localGet(ctx, k, true, command);
            }
         }
      }
   }

   private boolean isNotInL1(Object key) {
      return !isL1CacheEnabled || !dataContainer.containsKey(key);
   }

   private Object remoteGetAndStoreInL1(InvocationContext ctx, Object key, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      boolean isKeyLocalToNode = dm.getReadConsistentHash().isKeyLocalToNode(rpcManager.getAddress(), key);

      if (ctx.isOriginLocal() && !isKeyLocalToNode && isNotInL1(key) || dm.isAffectedByRehash(key) && !dataContainer.containsKey(key)) {
         if (trace) log.tracef("Doing a remote get for key %s", key);

         boolean acquireRemoteLock = false;
         if (ctx.isInTxScope()) {
            TxInvocationContext txContext = (TxInvocationContext) ctx;
            acquireRemoteLock = isWrite && isPessimisticCache && !txContext.getAffectedKeys().contains(key);
         }
         // attempt a remote lookup
         InternalCacheEntry ice = retrieveFromRemoteSource(key, ctx, acquireRemoteLock, command);

         if (acquireRemoteLock) {
            ((TxInvocationContext) ctx).addAffectedKey(key);
         }

         if (ice != null) {
            if (useClusteredWriteSkewCheck && ctx.isInTxScope()) {
               ((TxInvocationContext)ctx).getCacheTransaction().putLookedUpRemoteVersion(key, ice.getVersion());
            }

            if (isL1CacheEnabled) {
               // We've requested the key only from the owners current (read) CH.
               // If the intersection of owners in the current and pending CHs is empty,
               // the requestor information might be lost, so we shouldn't store the entry in L1.
               if (dm.isAffectedByRehash(key)) {
                  if (trace) log.tracef("State transfer in progress for key %s, not storing to L1");
                  return ice.getValue();
               }

               if (trace) log.tracef("Caching remotely retrieved entry for key %s in L1", key);
               // This should be fail-safe
               try {
                  long l1Lifespan = cacheConfiguration.clustering().l1().lifespan();
                  long lifespan = ice.getLifespan() < 0 ? l1Lifespan : Math.min(ice.getLifespan(), l1Lifespan);
                  PutKeyValueCommand put = cf.buildPutKeyValueCommand(ice.getKey(), ice.getValue(), lifespan, -1, command.getFlags());
                  lockAndWrap(ctx, key, ice, command);
                  invokeNextInterceptor(ctx, put);
               } catch (Exception e) {
                  // Couldn't store in L1 for some reason.  But don't fail the transaction!
                  log.infof("Unable to store entry %s in L1 cache", key);
                  log.debug("Inability to store in L1 caused by", e);
               }
            } else {
               if (!ctx.replaceValue(key, ice)) {
                  if (isWrite)
                     lockAndWrap(ctx, key, ice, command);
                  else
                     ctx.putLookedUpEntry(key, ice);
               }
            }
            return ice.getValue();
         }
      } else {
         if (trace) log.tracef("Not doing a remote get for key %s since entry is mapped to current node (%s), or is in L1.  Owners are %s", key, rpcManager.getAddress(), dm.locate(key));
      }
      return null;
   }

   protected Future<?> flushL1Caches(InvocationContext ctx) {
      // TODO how do we tell the L1 manager which keys are removed and which keys may still exist in remote L1?
      return isL1CacheEnabled ? l1Manager.flushCacheWithSimpleFuture(ctx.getLockedKeys(), null, ctx.getOrigin(), true) : null;
   }
}
