package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.container.EntryFactory;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;
import org.infinispan.util.concurrent.AggregatingNotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingFutureImpl;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The interceptor that handles distribution of entries across a cluster, as well as transparent lookup
 *
 * @author manik
 * @since 4.0
 */
public class DistributionInterceptor extends BaseRpcInterceptor {
   DistributionManager dm;
   CommandsFactory cf;
   DataContainer dataContainer;
   boolean isL1CacheEnabled, needReliableReturnValues;
   EntryFactory entryFactory;


   static final RecipientGenerator CLEAR_COMMAND_GENERATOR = new RecipientGenerator() {
      private final Object[] EMPTY_ARRAY = {};

      public List<Address> generateRecipients() {
         return null;
      }

      public Object[] getKeys() {
         return EMPTY_ARRAY;
      }
   };

   @Inject
   public void injectDependencies(DistributionManager distributionManager, CommandsFactory cf, DataContainer dataContainer, EntryFactory entryFactory) {
      this.dm = distributionManager;
      this.cf = cf;
      this.dataContainer = dataContainer;
      this.entryFactory = entryFactory;
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
      boolean isStillRehashingOnJoin = !dm.isJoinComplete();
      Object returnValue = invokeNextInterceptor(ctx, command);
      // need to check in the context as well since a null retval is not necessarily an indication of the entry not being
      // available.  It could just have been removed in the same tx beforehand.
      if (!ctx.hasFlag(Flag.SKIP_REMOTE_LOOKUP) && returnValue == null && ctx.lookupEntry(command.getKey()) == null)
         returnValue = remoteGetAndStoreInL1(ctx, command.getKey(), isStillRehashingOnJoin);
      return returnValue;
   }

   /**
    * This method retrieves an entry from a remote cache and optionally stores it in L1 (if L1 is enabled).
    * <p/>
    * This method only works if a) this is a locally originating invocation and b) the entry in question is not local to
    * the current cache instance and c) the entry is not in L1.  If either of a, b or c does not hold true, this method
    * returns a null and doesn't do anything.
    *
    * @param ctx invocation context
    * @param key key to retrieve
    * @return value of a remote get, or null
    * @throws Throwable if there are problems
    */
   private Object remoteGetAndStoreInL1(InvocationContext ctx, Object key, boolean dmWasRehashingDuringLocalLookup) throws Throwable {
      boolean isMappedToLocalNode = false;
      if (ctx.isOriginLocal() && !(isMappedToLocalNode = dm.isLocal(key)) && isNotInL1(key)) {
         return realRemoteGet(ctx, key, true);
      } else {
         // maybe we are still rehashing as a joiner? ISPN-258
         if (isMappedToLocalNode && dmWasRehashingDuringLocalLookup) {
            if (trace)
               log.trace("Key is mapped to local node, but a rehash is in progress so may need to look elsewhere");
            // try a remote lookup all the same
            return realRemoteGet(ctx, key, false);
         } else {
            if (trace)
               log.trace("Not doing a remote get for key {0} since entry is mapped to current node, or is in L1", key);
         }
      }
      return null;
   }

   private Object realRemoteGet(InvocationContext ctx, Object key, boolean storeInL1) throws Throwable {
      if (trace) log.trace("Doing a remote get for key {0}", key);
      // attempt a remote lookup
      InternalCacheEntry ice = dm.retrieveFromRemoteSource(key);

      if (ice != null) {
         if (storeInL1 && isL1CacheEnabled) {
            if (trace) log.trace("Caching remotely retrieved entry for key {0} in L1", key);
            long lifespan = ice.getLifespan() < 0 ? configuration.getL1Lifespan() : Math.min(ice.getLifespan(), configuration.getL1Lifespan());
            PutKeyValueCommand put = cf.buildPutKeyValueCommand(ice.getKey(), ice.getValue(), lifespan, -1);
            entryFactory.wrapEntryForWriting(ctx, key, true, false, ctx.hasLockedKey(key), false, false);
            invokeNextInterceptor(ctx, put);
         } else {
            if (trace) log.trace("Not caching remotely retrieved entry for key {0} in L1", key);
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
      return handleWriteCommand(ctx, command, new SingleKeyRecipientGenerator(command.getKey()), false);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      // don't bother with a remote get for the PutMapCommand!
      return handleWriteCommand(ctx, command,
                                new MultipleKeysRecipientGenerator(command.getMap().keySet()), true);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {

      return handleWriteCommand(ctx, command,
                                new SingleKeyRecipientGenerator(command.getKey()), false);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleWriteCommand(ctx, command, CLEAR_COMMAND_GENERATOR, false);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command,
                                new SingleKeyRecipientGenerator(command.getKey()), false);
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (ctx.isOriginLocal())
         rpcManager.invokeRemotely(dm.getAffectedNodes(ctx.getAffectedKeys()), command, true, true);
      return invokeNextInterceptor(ctx, command);
   }

   // ---- TX boundary commands

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (ctx.isOriginLocal() && ctx.hasModifications()) {
         List<Address> recipients = dm.getAffectedNodes(ctx.getAffectedKeys());
         NotifyingNotifiableFuture<Object> f = flushL1Cache(recipients.size(), getKeys(ctx.getModifications()), null);
         rpcManager.invokeRemotely(recipients, command, configuration.isSyncCommitPhase(), true);
         if (f != null) f.get();
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);

      boolean sync = isSynchronous(ctx);

      if (ctx.isOriginLocal() && ctx.hasModifications()) {
         List<Address> recipients = dm.getAffectedNodes(ctx.getAffectedKeys());
         NotifyingNotifiableFuture<Object> f = null;
         if (command.isOnePhaseCommit())
            f = flushL1Cache(recipients.size(), getKeys(ctx.getModifications()), null);
         // this method will return immediately if we're the only member (because exclude_self=true)
         rpcManager.invokeRemotely(recipients, command, sync);
         if (f != null) f.get();
      }
      return retVal;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (ctx.isOriginLocal())
         rpcManager.invokeRemotely(dm.getAffectedNodes(ctx.getAffectedKeys()), command, configuration.isSyncRollbackPhase(), true);
      return invokeNextInterceptor(ctx, command);
   }

   private void remoteGetBeforeWrite(InvocationContext ctx, boolean isConditionalCommand, KeyGenerator keygen) throws Throwable {
      // this should only happen if:
      //   a) unsafeUnreliableReturnValues is false
      //   b) unsafeUnreliableReturnValues is true, we are in a TX and the command is conditional
      boolean isStillRehashingOnJoin = !dm.isJoinComplete();
      if (isNeedReliableReturnValues(ctx) || (isConditionalCommand && ctx.isInTxScope())) {
         for (Object k : keygen.getKeys()) remoteGetAndStoreInL1(ctx, k, isStillRehashingOnJoin);
      }
   }

   private boolean isNeedReliableReturnValues(InvocationContext ctx) {
      return !ctx.hasFlag(Flag.SKIP_REMOTE_LOOKUP) && needReliableReturnValues;
   }

   private Object[] getKeys(List<WriteCommand> mods) {
      List<Object> l = new LinkedList<Object>();
      for (WriteCommand m : mods) {
         if (m instanceof DataCommand) {
            l.add(((DataCommand) m).getKey());
         } else if (m instanceof PutMapCommand) {
            l.addAll(((PutMapCommand) m).getMap().keySet());
         }
      }
      return l.toArray(new Object[l.size()]);
   }

   private NotifyingNotifiableFuture<Object> flushL1Cache(int numCallRecipients, Object[] keys, Object retval) {
      if (isL1CacheEnabled && numCallRecipients > 0 && rpcManager.getTransport().getMembers().size() > numCallRecipients) {
         if (trace) log.trace("Invalidating L1 caches");
         InvalidateCommand ic = cf.buildInvalidateFromL1Command(false, keys);
         NotifyingNotifiableFuture<Object> future = new AggregatingNotifyingFutureImpl(retval, 2);
         rpcManager.broadcastRpcCommandInFuture(ic, future);
         return future;
      }
      return null;
   }

   /**
    * If we are within one transaction we won't do any replication as replication would only be performed at commit
    * time. If the operation didn't originate locally we won't do any replication either.
    */
   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator, boolean skipRemoteGet) throws Throwable {
      // TODO Remove isSingleOwnerAndLocal() once https://jira.jboss.org/jira/browse/JGRP-1084 has been implemented
      boolean localModeForced = isLocalModeForced(ctx) || isSingleOwnerAndLocal(recipientGenerator);
      // see if we need to load values from remote srcs first
      if (!skipRemoteGet) remoteGetBeforeWrite(ctx, command.isConditional(), recipientGenerator);

      // if this is local mode then skip distributing
      if (localModeForced) {
         log.trace("LOCAL mode forced.  No RPC needed.");
         return invokeNextInterceptor(ctx, command);
      }

      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to distribute.
      Object returnValue = invokeNextInterceptor(ctx, command);

      if (command.isSuccessful()) {
         if (!ctx.isInTxScope()) {
            if (ctx.isOriginLocal()) {
               List<Address> rec = recipientGenerator.generateRecipients();
               if (trace) log.trace("Invoking command {0} on hosts {1}", command, rec);
               boolean useFuture = ctx.isUseFutureReturnType();
               boolean sync = isSynchronous(ctx);
               NotifyingNotifiableFuture<Object> future = flushL1Cache(rec == null ? 0 : rec.size(), recipientGenerator.getKeys(), returnValue);
               if (useFuture) {
                  if (future == null) future = new NotifyingFutureImpl(returnValue);
                  rpcManager.invokeRemotelyInFuture(rec, command, future);
                  return future;
               } else {
                  rpcManager.invokeRemotely(rec, command, sync);
                  if (future != null && !sync) future.get(); // wait for the inval command to complete
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
      return configuration.getNumOwners() == 1 && (recipients = recipientGenerator.generateRecipients()) != null && recipients.get(0).equals(rpcManager.getTransport().getAddress());
   }

   interface KeyGenerator {
      Object[] getKeys();
   }

   interface RecipientGenerator extends KeyGenerator {
      List<Address> generateRecipients();
   }

   class SingleKeyRecipientGenerator implements RecipientGenerator {
      final Object key;
      final Object[] keysArray;
      List<Address> recipients = null;

      SingleKeyRecipientGenerator(Object key) {
         this.key = key;
         keysArray = new Object[]{key};
      }

      public List<Address> generateRecipients() {
         if (recipients == null) recipients = dm.locate(key);
         return recipients;
      }

      public Object[] getKeys() {
         return keysArray;
      }
   }

   class MultipleKeysRecipientGenerator implements RecipientGenerator {

      final Collection<Object> keys;
      final Object[] keysArray;
      List<Address> recipients = null;

      MultipleKeysRecipientGenerator(Collection<Object> keys) {
         this.keys = keys;
         keysArray = keys.toArray();
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

      public Object[] getKeys() {
         return keysArray;
      }
   }
}
