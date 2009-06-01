package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.LockControlCommand;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
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
   public void injectDependencies(DistributionManager distributionManager, CommandsFactory cf, DataContainer dataContainer) {
      this.dm = distributionManager;
      this.cf = cf;
      this.dataContainer = dataContainer;
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
      Object returnValue = invokeNextInterceptor(ctx, command);
      // need to check in the context as well since a null retval is not necessarily an indication of the entry not being
      // available.  It could just have been removed in the same tx beforehand.
      if (!ctx.hasFlag(Flag.SKIP_REMOTE_LOOKUP) && returnValue == null && ctx.lookupEntry(command.getKey()) == null)
         returnValue = remoteGetAndStoreInL1(ctx, command.getKey());
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
   private Object remoteGetAndStoreInL1(InvocationContext ctx, Object key) throws Throwable {
      if (ctx.isOriginLocal() && !dm.isLocal(key) && isNotInL1(key)) {
         if (trace) log.trace("Doing a remote get for key {0}", key);
         // attempt a remote lookup
         InternalCacheEntry ice = dm.retrieveFromRemoteSource(key);

         if (ice != null) {
            if (isL1CacheEnabled) {
               if (trace) log.trace("Caching remotely retrieved entry for key {0} in L1", key);
               long lifespan = ice.getLifespan() < 0 ? configuration.getL1Lifespan() : Math.min(ice.getLifespan(), configuration.getL1Lifespan());
               PutKeyValueCommand put = cf.buildPutKeyValueCommand(ice.getKey(), ice.getValue(), lifespan, -1);
               invokeNextInterceptor(ctx, put);
            } else {
               if (trace) log.trace("Not caching remotely retrieved entry for key {0} in L1", key);
            }
            return ice.getValue();
         }

      } else {
         if (trace)
            log.trace("Not doing a remote get for key {0} since entry is mapped to current node, or is in L1", key);
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
      return handleWriteCommand(ctx, command, new SingleKeyRecipientGenerator(command.getKey()));
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWriteCommand(ctx, command,
                                new MultipleKeysRecipientGenerator(command.getMap().keySet()));
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {

      return handleWriteCommand(ctx, command,
                                new SingleKeyRecipientGenerator(command.getKey()));
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleWriteCommand(ctx, command, CLEAR_COMMAND_GENERATOR);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWriteCommand(ctx, command,
                                new SingleKeyRecipientGenerator(command.getKey()));
   }
   
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         List<Address> recipients = new ArrayList<Address>(ctx.getTransactionParticipants());
         rpcManager.invokeRemotely(recipients, command, true, true);
      }
      return invokeNextInterceptor(ctx, command);
   }

   // ---- TX boundard commands
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         List<Address> recipients = new ArrayList<Address>(ctx.getTransactionParticipants());
         rpcManager.invokeRemotely(recipients, command, configuration.isSyncCommitPhase(), true);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);

      boolean sync = isSynchronous(ctx);

      if (ctx.isOriginLocal()) {
         List<Address> recipients = new ArrayList<Address>(ctx.getTransactionParticipants());
         if (trace) log.trace("Multicasting PrepareCommand to recipients : " + recipients);
         // this method will return immediately if we're the only member (because exclude_self=true)
         rpcManager.invokeRemotely(recipients, command, sync);
      }
      return retVal;
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         List<Address> recipients = new ArrayList<Address>(ctx.getTransactionParticipants());
         rpcManager.invokeRemotely(recipients, command, configuration.isSyncRollbackPhase(), true);
      }
      return invokeNextInterceptor(ctx, command);
   }

   private void remoteGetBeforeWrite(InvocationContext ctx, boolean isConditionalCommand, Object... keys) throws Throwable {
      // this should only happen if:
      //   a) unsafeUnreliableReturnValues is false
      //   b) unsafeUnreliableReturnValues is true, we are in a TX and the command is conditional

      if (isNeedReliableReturnValues(ctx) || (isConditionalCommand && ctx.isInTxScope())) {
         for (Object k : keys) remoteGetAndStoreInL1(ctx, k);
      }
   }

   private boolean isNeedReliableReturnValues(InvocationContext ctx) {
      return !ctx.hasFlag(Flag.SKIP_REMOTE_LOOKUP) && needReliableReturnValues;
   }

   /**
    * If we are within one transaction we won't do any replication as replication would only be performed at commit
    * time. If the operation didn't originate locally we won't do any replication either.
    */
   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator) throws Throwable {
      boolean localModeForced = isLocalModeForced(ctx);
      // see if we need to load values from remote srcs first
      remoteGetBeforeWrite(ctx, command.isConditional(), recipientGenerator.getKeys());

      // if this is local mode then skip distributing
      if (localModeForced && ctx.isInTxScope()) return invokeNextInterceptor(ctx, command);

      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to distribute.
      Object returnValue = invokeNextInterceptor(ctx, command);

      if (command.isSuccessful()) {
         if (!ctx.isInTxScope()) {
            if (ctx.isOriginLocal()) {
               List<Address> rec = recipientGenerator.generateRecipients();
               if (trace) log.trace("Invoking command {0} on hosts {1}", command, rec);
               boolean useFuture = ctx.isUseFutureReturnType();
               boolean sync = isSynchronous(ctx);
               NotifyingNotifiableFuture<Object> future = null;
               // if L1 caching is used make sure we broadcast an invalidate message
               if (isL1CacheEnabled && rec != null && rpcManager.getTransport().getMembers().size() > rec.size()) {
                  InvalidateCommand ic = cf.buildInvalidateFromL1Command(recipientGenerator.getKeys());
                  if (useFuture) {
                     future = new AggregatingNotifyingFutureImpl(returnValue, 2);
                     rpcManager.broadcastRpcCommandInFuture(ic, future);
                  } else {
                     rpcManager.broadcastRpcCommand(ic, sync);
                  }
               }

               if (useFuture) {
                  if (future == null) future = new NotifyingFutureImpl(returnValue);
                  rpcManager.invokeRemotelyInFuture(rec, command, future);
                  return future;
               } else {
                  rpcManager.invokeRemotely(rec, command, sync);
               }
            }
         } else {
            if (!localModeForced) {
               ((TxInvocationContext) ctx).addTransactionParticipants(recipientGenerator.generateRecipients());
            }
         }
      }
      return returnValue;
   }

   interface RecipientGenerator {
      List<Address> generateRecipients();

      Object[] getKeys();
   }

   class SingleKeyRecipientGenerator implements RecipientGenerator {
      Object key;

      SingleKeyRecipientGenerator(Object key) {
         this.key = key;
      }

      public List<Address> generateRecipients() {
         return dm.locate(key);
      }

      public Object[] getKeys() {
         return new Object[]{key};
      }
   }

   class MultipleKeysRecipientGenerator implements RecipientGenerator {

      Collection<Object> keys;

      MultipleKeysRecipientGenerator(Collection<Object> keys) {
         this.keys = keys;
      }

      public List<Address> generateRecipients() {
         Set<Address> addresses = new HashSet<Address>();
         Map<Object, List<Address>> recipients = dm.locateAll(keys);
         for (List<Address> a : recipients.values()) addresses.addAll(a);
         return Immutables.immutableListConvert(addresses);
      }

      public Object[] getKeys() {
         return keys.toArray();
      }
   }
}
