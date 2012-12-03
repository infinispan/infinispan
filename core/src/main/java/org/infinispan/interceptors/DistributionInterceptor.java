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
package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.AbstractDataCommand;
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
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DataLocality;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.L1Manager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.SuspectException;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.Immutables;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

/**
 * The interceptor that handles distribution of entries across a cluster, as well as transparent lookup
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Pete Muir
 * @author Dan Berindei <dan@infinispan.org>
 * @since 4.0
 */
public class DistributionInterceptor extends BaseRpcInterceptor {
   private DistributionManager dm;
   private CommandsFactory cf;
   private DataContainer dataContainer;
   private boolean isL1CacheEnabled, needReliableReturnValues;
   private EntryFactory entryFactory;
   private L1Manager l1Manager;
   private LockManager lockManager;

   static final RecipientGenerator CLEAR_COMMAND_GENERATOR = new RecipientGenerator() {
      @Override
      public List<Address> generateRecipients() {
         return null;
      }

      @Override
      public Collection<Object> getKeys() {
         return InfinispanCollections.emptySet();
      }
   };
   private boolean isPessimisticCache;

   private static final Log log = LogFactory.getLog(DistributionInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void injectDependencies(DistributionManager distributionManager,
                                  CommandsFactory cf, DataContainer dataContainer, EntryFactory entryFactory,
                                  L1Manager l1Manager, LockManager lockManager) {
      this.dm = distributionManager;
      this.cf = cf;
      this.dataContainer = dataContainer;
      this.entryFactory = entryFactory;
      this.l1Manager = l1Manager;
      this.lockManager = lockManager;
   }

   @Start
   public void start() {
      isL1CacheEnabled = cacheConfiguration.clustering().l1().enabled();
      needReliableReturnValues = !cacheConfiguration.unsafe().unreliableReturnValues();
      isPessimisticCache = cacheConfiguration.transaction().lockingMode() == LockingMode.PESSIMISTIC;
   }

   // ---- READ commands

   // if we don't have the key locally, fetch from one of the remote servers
   // and if L1 is enabled, cache in L1
   // then return

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
         if (needsRemoteGet(ctx, command, returnValue == null))
            returnValue = remoteGetAndStoreInL1(ctx, command.getKey(), false, command);
         return returnValue;
      } catch (SuspectException e) {
         // retry
         return visitGetKeyValueCommand(ctx, command);
      }
   }

   private boolean needsRemoteGet(InvocationContext ctx, AbstractDataCommand command, boolean retvalCheck) {
      final CacheEntry entry;
      return retvalCheck
            && !command.hasFlag(Flag.CACHE_MODE_LOCAL)
            && !command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
            && !command.hasFlag(Flag.IGNORE_RETURN_VALUES)
            && ((entry = ctx.lookupEntry(command.getKey())) == null || entry.isNull() || entry.isLockPlaceholder());   //todo [anistor] this condition seems wrong
   }


   /**
    * This method retrieves an entry from a remote cache and optionally stores it in L1 (if L1 is enabled).
    * <p/>
    * This method only works if a) this is a locally originating invocation and b) the entry in question is not local to
    * the current cache instance and c) the entry is not in L1.  If either of a, b or c does not hold true, this method
    * returns a null and doesn't do anything.
    *
    *
    * @param ctx invocation context
    * @param key key to retrieve
    * @return value of a remote get, or null
    * @throws Throwable if there are problems
    */
   private Object remoteGetAndStoreInL1(InvocationContext ctx, Object key, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      //DataLocality locality = dm.getLocality(key);
      DataLocality locality = dm.getReadConsistentHash().isKeyLocalToNode(rpcManager.getAddress(), key) ? DataLocality.LOCAL : DataLocality.NOT_LOCAL;

      if (ctx.isOriginLocal() && !locality.isLocal() && isNotInL1(key)) {
         return realRemoteGet(ctx, key, true, isWrite, command);
      } else {
         // maybe we are still rehashing as a joiner? ISPN-258
         if (locality.isUncertain()) {
            if (trace)
               log.tracef("Key %s is mapped to local node %s, but a rehash is in progress so may need to look elsewhere", key, rpcManager.getAddress());
            // try a remote lookup all the same
            return realRemoteGet(ctx, key, false, isWrite, command);
         } else {
            if (trace)
               log.tracef("Not doing a remote get for key %s since entry is mapped to current node (%s), or is in L1.  Owners are %s", key, rpcManager.getAddress(), dm.locate(key));
         }
      }
      return null;
   }

   private Object realRemoteGet(InvocationContext ctx, Object key, boolean storeInL1, boolean isWrite, FlagAffectedCommand command) throws Throwable {
      if (trace) log.tracef("Doing a remote get for key %s", key);

      boolean acquireRemoteLock = false;
      if (ctx.isInTxScope()) {
         TxInvocationContext txContext = (TxInvocationContext) ctx;
         acquireRemoteLock = isWrite && isPessimisticCache && !txContext.getAffectedKeys().contains(key);
      }
      // attempt a remote lookup
      InternalCacheEntry ice = dm.retrieveFromRemoteSource(key, ctx, acquireRemoteLock, command);

      if (acquireRemoteLock) {
         ((TxInvocationContext)ctx).addAffectedKey(key);
      }


      if (ice != null) {
         if (storeInL1) {
            if (isL1CacheEnabled) {
               // We've requested the key only from the owners current (read) CH.
               // If the intersection of owners in the current and pending CHs is empty,
               // the requestor information might be lost, so we shouldn't store the entry in L1.
               // TODO We don't have access to the pending CH here, so we just check if the owners list changed.
               List<Address> readOwners = dm.getReadConsistentHash().locateOwners(key);
               List<Address> writeOwners = dm.getWriteConsistentHash().locateOwners(key);
               if (!readOwners.equals(writeOwners)) {
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
               CacheEntry ce = ctx.lookupEntry(key);
               if (ce == null || ce.isNull() || ce.isLockPlaceholder() || ce.getValue() == null) {
                  if (ce != null && ce.isChanged()) {
                     ce.setValue(ice.getValue());
                  } else {
                     if (isWrite)
                        lockAndWrap(ctx, key, ice, command);
                     else
                        ctx.putLookedUpEntry(key, ice);
                  }
               }
            }
         } else {
            if (trace) log.tracef("Not caching remotely retrieved entry for key %s in L1", key);
         }
         return ice.getValue();
      }
      return null;
   }

   private void lockAndWrap(InvocationContext ctx, Object key, InternalCacheEntry ice, FlagAffectedCommand command) throws InterruptedException {
      boolean skipLocking = hasSkipLocking(command);
      long lockTimeout = getLockAcquisitionTimeout(command, skipLocking);
      lockManager.acquireLock(ctx, key, lockTimeout, skipLocking);
      entryFactory.wrapEntryForPut(ctx, key, ice, false, command);
   }

   /**
    * Tests whether a key is in the L1 cache if L1 is enabled.
    *
    * @param key key to check for
    * @return true if the key is not in L1, or L1 caching is not enabled.  false the key is in L1.
    */
   private boolean isNotInL1(Object key) {
      return !isL1CacheEnabled || !dataContainer.containsKey(key);
   }

   // ---- WRITE commands

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      SingleKeyRecipientGenerator skrg = new SingleKeyRecipientGenerator(command.getKey());
      Object returnValue = handleWriteCommand(ctx, command, skrg, false, false);
      // If this was a remote put record that which sent it
      if (isL1CacheEnabled && !ctx.isOriginLocal() && !skrg.generateRecipients().contains(ctx.getOrigin()))
         l1Manager.addRequestor(command.getKey(), ctx.getOrigin());

      return returnValue;
   }
   
   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // don't bother with a remote get for the PutMapCommand!
      return handleWriteCommand(ctx, command,
                                new MultipleKeysRecipientGenerator(command.getMap().keySet()), true, false);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {

      return handleWriteCommand(ctx, command,
                                new SingleKeyRecipientGenerator(command.getKey()), false, false);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleWriteCommand(ctx, command, CLEAR_COMMAND_GENERATOR, false, true);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command,
                                new SingleKeyRecipientGenerator(command.getKey()), false, false);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         final Collection<Address> affectedNodes = dm.getAffectedNodes(command.getKeys());
         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(affectedNodes);
         rpcManager.invokeRemotely(affectedNodes, command, true, true);
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

   private Future<?> flushL1Caches(InvocationContext ctx) {
      // TODO how do we tell the L1 manager which keys are removed and which keys may still exist in remote L1?
      return isL1CacheEnabled ? l1Manager.flushCacheWithSimpleFuture(ctx.getLockedKeys(), null, ctx.getOrigin(), true) : null;
   }

   private void blockOnL1FutureIfNeeded(Future<?> f) {
      if (f != null && cacheConfiguration.transaction().syncCommitPhase()) {
         try {
            f.get();
         } catch (Exception e) {
            // Ignore SuspectExceptions - if the node has gone away then there is nothing to invalidate anyway.
            if (!(e.getCause() instanceof SuspectException)) {
               log.failedInvalidatingRemoteCache(e);
            }
         }
      }
   }

   private void sendCommitCommand(TxInvocationContext ctx, CommitCommand command) throws TimeoutException, InterruptedException {
      Collection<Address> recipients = getCommitNodes(ctx);
      boolean syncCommitPhase = cacheConfiguration.transaction().syncCommitPhase();
      rpcManager.invokeRemotely(recipients, command, syncCommitPhase, true);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);

      if (shouldInvokeRemoteTxCommand(ctx)) {
         if (command.isOnePhaseCommit()) flushL1Caches(ctx); // if we are one-phase, don't block on this future.

         boolean affectsAllNodes = ctx.getCacheTransaction().hasModification(ClearCommand.class);
         Collection<Address> recipients = affectsAllNodes ? dm.getConsistentHash().getMembers() : dm.getAffectedNodes(ctx.getAffectedKeys());
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
      rpcManager.invokeRemotely(recipients, command, sync);
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         rpcManager.invokeRemotely(getCommitNodes(ctx), command, cacheConfiguration.transaction().syncRollbackPhase(), true);
      }

      return invokeNextInterceptor(ctx, command);
   }

   private void remoteGetBeforeWrite(InvocationContext ctx, WriteCommand command, KeyGenerator keygen) throws Throwable {
      // this should only happen if:
      //   a) unsafeUnreliableReturnValues is false
      //   b) unsafeUnreliableReturnValues is true, we are in a TX and the command is conditional
      if (isNeedReliableReturnValues(command) || (command.isConditional() && ctx.isInTxScope())) {
         for (Object k : keygen.getKeys())
            remoteGetAndStoreInL1(ctx, k, true, command);
      }
   }

   private boolean isNeedReliableReturnValues(FlagAffectedCommand command) {
      return !command.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
            && !command.hasFlag(Flag.IGNORE_RETURN_VALUES) && needReliableReturnValues;
   }

   /**
    * If we are within one transaction we won't do any replication as replication would only be performed at commit
    * time. If the operation didn't originate locally we won't do any replication either.
    */
   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator, boolean skipRemoteGet, boolean skipL1Invalidation) throws Throwable {
      // see if we need to load values from remote srcs first
      if (ctx.isOriginLocal() && !skipRemoteGet)
         remoteGetBeforeWrite(ctx, command, recipientGenerator);
      boolean sync = isSynchronous(command);

      // if this is local mode then skip distributing
      if (isLocalModeForced(command)) {
         return invokeNextInterceptor(ctx, command);
      }

      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to distribute.
      Object returnValue = invokeNextInterceptor(ctx, command);

      if (command.isSuccessful()) {

         if (!ctx.isInTxScope()) {
            NotifyingNotifiableFuture<Object> futureToReturn = null;
            Future<?> invalidationFuture = null;
            if (ctx.isOriginLocal()) {
               List<Address> rec = recipientGenerator.generateRecipients();
               int numCallRecipients = rec == null ? 0 : rec.size();
               if (trace) log.tracef("Invoking command %s on hosts %s", command, rec);

               boolean useFuture = ctx.isUseFutureReturnType();
               if (isL1CacheEnabled && !skipL1Invalidation)
               	// Handle the case where the put is local. If in unicast mode and this is not a data
               	// owner, nothing happens. If in multicast mode, we this node will send the multicast
               	if (rpcManager.getTransport().getMembers().size() > numCallRecipients) {
               		// Command was successful, we have a number of receipients and L1 should be flushed, so request any L1 invalidations from this node
               		if (trace) log.tracef("Put occurring on node, requesting L1 cache invalidation for keys %s. Other data owners are %s", command.getAffectedKeys(), dm.getAffectedNodes(command.getAffectedKeys()));
                     if (useFuture) {
               		   futureToReturn = l1Manager.flushCache(recipientGenerator.getKeys(), returnValue,
                                                              ctx.getOrigin(), !(command instanceof RemoveCommand));
                     } else {
                        invalidationFuture = l1Manager.flushCacheWithSimpleFuture(recipientGenerator.getKeys(), returnValue,
                                                                                  ctx.getOrigin(), !(command instanceof RemoveCommand));
                     }
               	} else {
                     if (trace) log.tracef("Not performing invalidation! numCallRecipients=%s", numCallRecipients);
                  }
               if (!isSingleOwnerAndLocal(recipientGenerator)) {
                  if (useFuture) {
                     if (futureToReturn == null) futureToReturn = new NotifyingFutureImpl(returnValue);
                     rpcManager.invokeRemotelyInFuture(rec, command, futureToReturn);
                     return futureToReturn;
                  } else {
                     rpcManager.invokeRemotely(rec, command, sync);
                  }
               } else if (useFuture && futureToReturn != null) {
                  return futureToReturn;
               }
               if (invalidationFuture != null && sync) {
                  invalidationFuture.get(); // wait for the inval command to complete
                  if (trace) log.tracef("Finished invalidating keys %s ", recipientGenerator.getKeys());
               }
            } else {
            	// Piggyback remote puts and cause L1 invalidations
            	if (isL1CacheEnabled && !skipL1Invalidation) {
               	// Command was successful and L1 should be flushed, so request any L1 invalidations from this node
            		if (trace) log.tracef("Put occurring on node, requesting cache invalidation for keys %s. Origin of command is remote", command.getAffectedKeys());
                  // If this is a remove command, then don't pass in the origin - since the entru would be removed from the origin's L1 cache.
            		invalidationFuture = l1Manager.flushCacheWithSimpleFuture(recipientGenerator.getKeys(),
                                                                            returnValue, ctx.getOrigin(), !(command instanceof RemoveCommand));
            		if (sync) {
                     try {
                        invalidationFuture.get(); // wait for the inval command to complete
                     } catch (ExecutionException ee) {
                        // Ignore SuspectExceptions - if the node has gone away then there is nothing to invalidate anyway.
                        if (!(ee.getCause() instanceof SuspectException)) {
                           throw ee.getCause();
                        }
                     }
                     if (trace) log.tracef("Finished invalidating keys %s ", recipientGenerator.getKeys());
            		}
            	}
            }
         }
      }
      return returnValue;
   }

   /**
    * If a single owner has been configured and the target for the key is the local address, it returns true.
    */
   private boolean isSingleOwnerAndLocal(RecipientGenerator recipientGenerator) {
      List<Address> recipients;
      return cacheConfiguration.clustering().hash().numOwners() == 1
            && (recipients = recipientGenerator.generateRecipients()) != null
            && recipients.size() == 1
            && recipients.get(0).equals(rpcManager.getTransport().getAddress());
   }

   private Collection<Address> getCommitNodes(TxInvocationContext ctx) {
      LocalTransaction localTx = (LocalTransaction) ctx.getCacheTransaction();
      Collection<Address> affectedNodes = dm.getAffectedNodes(ctx.getAffectedKeys());
      List<Address> members = dm.getConsistentHash().getMembers();
      return localTx.getCommitNodes(affectedNodes, rpcManager.getTopologyId(), members);
   }

   interface KeyGenerator {
      Collection<Object> getKeys();
   }

   interface RecipientGenerator extends KeyGenerator {
      List<Address> generateRecipients();
   }

   class SingleKeyRecipientGenerator implements RecipientGenerator {
      final Object key;
      final Set<Object> keys;
      List<Address> recipients = null;

      SingleKeyRecipientGenerator(Object key) {
         this.key = key;
         keys = Collections.singleton(key);
      }

      @Override
      public List<Address> generateRecipients() {
         if (recipients == null) recipients = dm.locate(key);
         return recipients;
      }

      @Override
      public Collection<Object> getKeys() {
         return keys;
      }
   }

   class MultipleKeysRecipientGenerator implements RecipientGenerator {

      final Collection<Object> keys;
      List<Address> recipients = null;

      MultipleKeysRecipientGenerator(Collection<Object> keys) {
         this.keys = keys;
      }

      @Override
      public List<Address> generateRecipients() {
         if (recipients == null) {
            Set<Address> addresses = dm.locateAll(keys);
            recipients = Immutables.immutableListConvert(addresses);
         }
         return recipients;
      }

      @Override
      public Collection<Object> getKeys() {
         return keys;
      }
   }

}
