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
import org.infinispan.distribution.RehashInProgressException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateTransferLock;
import org.infinispan.util.Immutables;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
   DistributionManager dm;
   StateTransferLock stateTransferLock;
   CommandsFactory cf;
   DataContainer dataContainer;
   boolean isL1CacheEnabled, needReliableReturnValues;
   EntryFactory entryFactory;
   L1Manager l1Manager;

   static final RecipientGenerator CLEAR_COMMAND_GENERATOR = new RecipientGenerator() {
      public List<Address> generateRecipients() {
         return null;
      }

      public Collection<Object> getKeys() {
         return Collections.emptySet();
      }
   };

   @Inject
   public void injectDependencies(DistributionManager distributionManager, StateTransferLock stateTransferLock,
                                  CommandsFactory cf, DataContainer dataContainer, EntryFactory entryFactory,
                                  L1Manager l1Manager) {
      this.dm = distributionManager;
      this.stateTransferLock = stateTransferLock;
      this.cf = cf;
      this.dataContainer = dataContainer;
      this.entryFactory = entryFactory;
      this.l1Manager = l1Manager;
   }

   @Start
   public void start() {
      isL1CacheEnabled = configuration.isL1CacheEnabled();
      needReliableReturnValues = !configuration.isUnsafeUnreliableReturnValues();
   }

   // ---- READ commands

   // if we don't have the key locally, fetch from one of the remote servers
   // and if L1 is enabled, cache in L1
   // then return

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      boolean isRehashInProgress = dm.isRehashInProgress();
      Object returnValue = invokeNextInterceptor(ctx, command);

      // If L1 caching is enabled, this is a remote command, and we found a value in our cache
      // we store it so that we can later invalidate it
      if (isL1CacheEnabled && !ctx.isOriginLocal() && returnValue != null) {
      	l1Manager.addRequestor(command.getKey(), ctx.getOrigin());
      }

      // need to check in the context as well since a null retval is not necessarily an indication of the entry not being
      // available.  It could just have been removed in the same tx beforehand.
      if (needsRemoteGet(ctx, command.getKey(), returnValue == null))
         returnValue = remoteGetAndStoreInL1(ctx, command.getKey(), false);
      return returnValue;
   }

   private boolean needsRemoteGet(InvocationContext ctx, Object key, boolean retvalCheck) {
      final CacheEntry entry;
      return retvalCheck
            && !ctx.hasFlag(Flag.CACHE_MODE_LOCAL)
            && !ctx.hasFlag(Flag.SKIP_REMOTE_LOOKUP)
            && ((entry = ctx.lookupEntry(key)) == null || entry.isNull() || entry.isLockPlaceholder());
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
   private Object remoteGetAndStoreInL1(InvocationContext ctx, Object key, boolean isWrite) throws Throwable {
      DataLocality locality = dm.getLocality(key);

      if (ctx.isOriginLocal() && !locality.isLocal() && isNotInL1(key)) {
         return realRemoteGet(ctx, key, true, isWrite);
      } else {
         // maybe we are still rehashing as a joiner? ISPN-258
         if (locality.isUncertain()) {
            if (trace)
               log.tracef("Key %s is mapped to local node %s, but a rehash is in progress so may need to look elsewhere", key, rpcManager.getAddress());
            // try a remote lookup all the same
            return realRemoteGet(ctx, key, false, isWrite);
         } else {
            if (trace)
               log.tracef("Not doing a remote get for key %s since entry is mapped to current node (%s), or is in L1.  Owners are %s", key, rpcManager.getAddress(), dm.locate(key));
         }
      }
      return null;
   }

   private Object realRemoteGet(InvocationContext ctx, Object key, boolean storeInL1, boolean isWrite) throws Throwable {
      if (trace) log.tracef("Doing a remote get for key %s", key);
      // attempt a remote lookup
      InternalCacheEntry ice = dm.retrieveFromRemoteSource(key, ctx);

      if (ice != null) {
         if (storeInL1) {
            if (isL1CacheEnabled) {
               if (trace) log.tracef("Caching remotely retrieved entry for key %s in L1", key);
               long lifespan = ice.getLifespan() < 0 ? configuration.getL1Lifespan() : Math.min(ice.getLifespan(), configuration.getL1Lifespan());
               PutKeyValueCommand put = cf.buildPutKeyValueCommand(ice.getKey(), ice.getValue(), lifespan, -1, ctx.getFlags());
               entryFactory.wrapEntryForWriting(ctx, key, true, false, ctx.hasLockedKey(key), false, false);
               invokeNextInterceptor(ctx, put);
            } else {
               CacheEntry ce = ctx.lookupEntry(key);
               if (ce == null || ce.isNull() || ce.isLockPlaceholder() || ce.getValue() == null) {
                  if (ce != null && ce.isChanged()) {
                     ce.setValue(ice.getValue());
                  } else {
                     if (isWrite)
                        entryFactory.wrapEntryForWriting(ctx, ice, true, false, ctx.hasLockedKey(key), false, false);
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
      Object returnValue = handleWriteCommand(ctx, command, new SingleKeyRecipientGenerator(command.getKey()), false, false);
      // If this was a remote put record that which sent it
      if (isL1CacheEnabled && !ctx.isOriginLocal()) {
      	l1Manager.addRequestor(command.getKey(), ctx.getOrigin());
      }
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
         if (configuration.isEagerLockSingleNode()) {
            //only main data owner is locked, see: https://jira.jboss.org/browse/ISPN-615
            Map<Object, List<Address>> toMulticast = dm.locateAll(command.getKeys(), 1);

            //now compile address reunion
            Collection<Address> where;
            if (toMulticast.size() == 1) {//avoid building an extra array, as most often this will be a single key
               where = toMulticast.values().iterator().next();
            } else {
               where = new HashSet<Address>();
               for (List<Address> values : toMulticast.values()) where.addAll(values);
            }
            rpcManager.invokeRemotely(where, command, true, true);
            ((LocalTxInvocationContext) ctx).remoteLocksAcquired(where);
         } else {
            rpcManager.invokeRemotely(dm.getAffectedNodes(command.getKeys()), command, true, true);
         }
         ctx.addAffectedKeys(command.getKeys());
      }
      return invokeNextInterceptor(ctx, command);
   }

   /**
    * If the response to a commit is a request to resend the prepare, respond accordingly *
    */
   private boolean needToResendPrepare(Response r) {
      return r instanceof SuccessfulResponse && Byte.valueOf(CommitCommand.RESEND_PREPARE).equals(((SuccessfulResponse) r).getResponseValue());
   }

   // ---- TX boundary commands
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> preparedOn = ((LocalTxInvocationContext) ctx).getRemoteLocksAcquired();

         NotifyingNotifiableFuture<Object> f = null;
         if (isL1CacheEnabled) {
            f = l1Manager.flushCache(ctx.getLockedKeys(), null, null);
         }

         // We try harder to make a commit succeed, but at some point we have to give up
         for (int i = 0; i < 3; i++) {
            boolean needToWaitForRehash = false;
            Collection<Address> recipients = dm.getAffectedNodes(ctx.getAffectedKeys());

            // By default, use the configured commit sync settings
            boolean syncCommitPhase = configuration.isSyncCommitPhase();
            for (Address a : preparedOn) {
               if (!recipients.contains(a)) {
                  // However if we have prepared on some nodes and are now committing on different nodes, make sure we
                  // force sync commit so we can respond to prepare resend requests.
                  syncCommitPhase = true;
               }
            }
            try {
               Map<Address, Response> responses = rpcManager.invokeRemotely(recipients, command, syncCommitPhase, true);
               if (!responses.isEmpty()) {
                  List<Address> resendTo = new LinkedList<Address>();
                  for (Map.Entry<Address, Response> r : responses.entrySet()) {
                     if (needToResendPrepare(r.getValue()))
                        resendTo.add(r.getKey());
                  }

                  if (!resendTo.isEmpty()) {
                     log.debugf("Need to resend prepares for %s to %s", command.getGlobalTransaction(), resendTo);
                     // Make sure this is 1-Phase!!
                     PrepareCommand pc = cf.buildPrepareCommand(command.getGlobalTransaction(), ctx.getModifications(), true);
                     rpcManager.invokeRemotely(resendTo, pc, true, true);
                  }
               }
               break;
            } catch (RehashInProgressException e) {
               // we are assuming the current node is also trying to start the rehash, but it can't
               // because we're holding the tx lock
               // there is no problem if some nodes already applied the commit
               stateTransferLock.waitForStateTransferToEnd(ctx, command);
               log.tracef("Rehashing completed, retrying the commit on the remote nodes %s", rpcManager.getAddress());
            }
         }

         if (f != null && configuration.isSyncCommitPhase()) {
            try {
               f.get();
            } catch (Exception e) {
               if (log.isInfoEnabled()) log.failedInvalidatingRemoteCache(e);
            }
         }
      }
      return invokeNextInterceptor(ctx, command);
   }


   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);

      boolean sync = isSynchronous(ctx);

      if (shouldInvokeRemoteTxCommand(ctx)) {
         Collection<Address> recipients = dm.getAffectedNodes(ctx.getAffectedKeys());
         NotifyingNotifiableFuture<Object> f = null;
         if (isL1CacheEnabled && command.isOnePhaseCommit())
            f = l1Manager.flushCache(ctx.getLockedKeys(), null, null);
         // this method will return immediately if we're the only member (because exclude_self=true)
         rpcManager.invokeRemotely(recipients, command, sync);
         ((LocalTxInvocationContext) ctx).remoteLocksAcquired(recipients);
         if (f != null) f.get();
      }
      return retVal;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (shouldInvokeRemoteTxCommand(ctx))
         rpcManager.invokeRemotely(dm.getAffectedNodes(ctx.getAffectedKeys()), command, configuration.isSyncRollbackPhase(), true);
      return invokeNextInterceptor(ctx, command);
   }

   private void remoteGetBeforeWrite(InvocationContext ctx, boolean isConditionalCommand, KeyGenerator keygen) throws Throwable {
      // this should only happen if:
      //   a) unsafeUnreliableReturnValues is false
      //   b) unsafeUnreliableReturnValues is true, we are in a TX and the command is conditional
      if (isNeedReliableReturnValues(ctx) || (isConditionalCommand && ctx.isInTxScope())) {
         boolean isStillRehashingOnJoin = !dm.isJoinComplete();
         for (Object k : keygen.getKeys()) remoteGetAndStoreInL1(ctx, k, true);
      }
   }

   private boolean isNeedReliableReturnValues(InvocationContext ctx) {
      return !ctx.hasFlag(Flag.SKIP_REMOTE_LOOKUP) && needReliableReturnValues;
   }

   /**
    * If we are within one transaction we won't do any replication as replication would only be performed at commit
    * time. If the operation didn't originate locally we won't do any replication either.
    */
   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator, boolean skipRemoteGet, boolean skipL1Invalidation) throws Throwable {
      // see if we need to load values from remote srcs first
      if (ctx.isOriginLocal() && !skipRemoteGet) remoteGetBeforeWrite(ctx, command.isConditional(), recipientGenerator);
      boolean sync = isSynchronous(ctx);

      // if this is local mode then skip distributing
      if (isLocalModeForced(ctx)) {
         return invokeNextInterceptor(ctx, command);
      }

      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to distribute.
      Object returnValue = invokeNextInterceptor(ctx, command);

      if (command.isSuccessful()) {

         if (!ctx.isInTxScope()) {
         	NotifyingNotifiableFuture<Object> future = null;
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
               		if (trace) log.tracef("Put occuring on node, requesting L1 cache invalidation for keys %s. Other data owners are %s", command.getAffectedKeys(), dm.getAffectedNodes(command.getAffectedKeys()));
               		future = l1Manager.flushCache(recipientGenerator.getKeys(), returnValue, null);
               	} else {
                     if (trace) log.tracef("Not performing invalidation! numCallRecipients=%s", numCallRecipients);
                  }
               if (!isSingleOwnerAndLocal(recipientGenerator)) {
                  if (useFuture) {
                     if (future == null) future = new NotifyingFutureImpl(returnValue);
                     rpcManager.invokeRemotelyInFuture(rec, command, future);
                     return future;
                  } else {
                     rpcManager.invokeRemotely(rec, command, sync);
                  }
               } else if (useFuture && future != null) {
                  return future;
               }
               if (future != null && sync) {
                  future.get(); // wait for the inval command to complete
                  if (trace) log.tracef("Finished invalidating keys %s ", recipientGenerator.getKeys());
               }
            } else {
            	// Piggyback remote puts and cause L1 invalidations
            	if (isL1CacheEnabled && !skipL1Invalidation) {
               	// Command was successful and L1 should be flushed, so request any L1 invalidations from this node
            		if (trace) log.tracef("Put occuring on node, requesting cache invalidation for keys %s. Origin of command is remote", command.getAffectedKeys());
            		future = l1Manager.flushCache(recipientGenerator.getKeys(), returnValue, ctx.getOrigin());
            		if (sync) {
            			future.get(); // wait for the inval command to complete
                     if (trace) log.tracef("Finished invalidating keys %s ", recipientGenerator.getKeys());
            		}
            	}
            }
         } else {
            ((TxInvocationContext) ctx).addAffectedKeys(recipientGenerator.getKeys());
         }
      }
      return returnValue;
   }

   /**
    * If a single owner has been configured and the target for the key is the local address, it returns true.
    */
   private boolean isSingleOwnerAndLocal(RecipientGenerator recipientGenerator) {
      List<Address> recipients;
      return configuration.getNumOwners() == 1 && (recipients = recipientGenerator.generateRecipients()) != null && recipients.size() == 1 && recipients.get(0).equals(rpcManager.getTransport().getAddress());
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

      public List<Address> generateRecipients() {
         if (recipients == null) recipients = dm.locate(key);
         return recipients;
      }

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

      public List<Address> generateRecipients() {
         if (recipients == null) {
            Set<Address> addresses = new HashSet<Address>();
            Map<Object, List<Address>> recipientsMap = dm.locateAll(keys);
            for (List<Address> a : recipientsMap.values()) addresses.addAll(a);
            recipients = Immutables.immutableListConvert(addresses);
         }
         return recipients;
      }

      public Collection<Object> getKeys() {
         return keys;
      }
   }

}
