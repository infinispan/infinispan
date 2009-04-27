package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
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
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.TransactionContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.BaseRpcInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Immutables;

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
   public void injectDependencies(DistributionManager distributionManager, CommandsFactory cf) {
      this.dm = distributionManager;
      this.cf = cf;
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
      if (returnValue == null && ctx.lookupEntry(command.getKey()) == null)
         returnValue = remoteGetAndStoreInL1(ctx, command.getKey());
      return returnValue;
   }

   private Object remoteGetAndStoreInL1(InvocationContext ctx, Object key) throws Throwable {
      if (ctx.isOriginLocal() && !dm.isLocal(key)) {
         if (trace) log.trace("Doing a remote get for key {0}", key);
         // attempt a remote lookup
         InternalCacheEntry ice = dm.retrieveFromRemoteSource(key);

         if (ice != null) {
            if (configuration.isL1CacheEnabled()) {
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

   // ---- WRITE commands

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWriteCommand(ctx, command,
                                new SingleKeyRecipientGenerator(command.getKey()));
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

   // ---- TX boundard commands
   @Override
   public Object visitCommitCommand(InvocationContext ctx, CommitCommand command) throws Throwable {
      if (!skipReplicationOfTransactionMethod(ctx)) {
         List<Address> recipients = new ArrayList<Address>(ctx.getTransactionContext().getTransactionParticipants());
         replicateCall(ctx, recipients, command, configuration.isSyncCommitPhase(), true);
      }
      return invokeNextInterceptor(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(InvocationContext ctx, PrepareCommand command) throws Throwable {
      Object retVal = invokeNextInterceptor(ctx, command);
      TransactionContext transactionContext = ctx.getTransactionContext();
      if (transactionContext.hasLocalModifications()) {
         PrepareCommand replicablePrepareCommand = command.copy(); // make sure we remove any "local" transactions
         replicablePrepareCommand.removeModifications(transactionContext.getLocalModifications());
         command = replicablePrepareCommand;
      }

      if (!skipReplicationOfTransactionMethod(ctx)) {
         boolean sync = configuration.getCacheMode().isSynchronous();
         if (trace) {
            log.trace("[" + rpcManager.getTransport().getAddress() + "] Running remote prepare for global tx {1}.  Synchronous? {2}",
                      rpcManager.getTransport().getAddress(), command.getGlobalTransaction(), sync);
         }

         List<Address> recipients = new ArrayList<Address>(ctx.getTransactionContext().getTransactionParticipants());

         // this method will return immediately if we're the only member (because exclude_self=true)
         replicateCall(ctx, recipients, command, sync, false);
      }

      return retVal;
   }

   @Override
   public Object visitRollbackCommand(InvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!skipReplicationOfTransactionMethod(ctx) && !ctx.isLocalRollbackOnly()) {
         List<Address> recipients = new ArrayList<Address>(ctx.getTransactionContext().getTransactionParticipants());
         replicateCall(ctx, recipients, command, configuration.isSyncRollbackPhase(), true);
      }
      return invokeNextInterceptor(ctx, command);
   }

   private void remoteGetBeforeWrite(InvocationContext ctx, Object... keys) throws Throwable {
      // only do this if we are sync (OR if we dont care about return values!)
//      if (!configuration.isUnsafeUnreliableReturnValues()) {
      for (Object k : keys) remoteGetAndStoreInL1(ctx, k);
//      }
   }

   /**
    * If we are within one transaction we won't do any replication as replication would only be performed at commit
    * time. If the operation didn't originate locally we won't do any replication either.
    */
   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator) throws Throwable {
      boolean local = isLocalModeForced(ctx);
      // see if we need to load values from remote srcs first
      remoteGetBeforeWrite(ctx, recipientGenerator.getKeys());

      // if this is local mode then skip distributing
      if (local && ctx.getTransaction() == null) return invokeNextInterceptor(ctx, command);

      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to distribute.
      Object returnValue = invokeNextInterceptor(ctx, command);

      if (command.isSuccessful()) {
         if (ctx.getTransaction() == null) {
            if (ctx.isOriginLocal()) {
               List<Address> rec = recipientGenerator.generateRecipients();
               if (trace) log.trace("Invoking command {0} on hosts {1}", command, rec);
               // if L1 caching is used make sure we broadcast an invalidate message
               if (configuration.isL1CacheEnabled() && rec != null) {
                  InvalidateCommand ic = cf.buildInvalidateFromL1Command(recipientGenerator.getKeys());
                  replicateCall(ctx, ic, isSynchronous(ctx), false);
               }
               replicateCall(ctx, rec, command, isSynchronous(ctx), false);
            }
         } else {
            if (local) {
               ctx.getTransactionContext().addLocalModification(command);
            } else {
               // add to list of participants
               ctx.getTransactionContext().addTransactionParticipants(recipientGenerator.generateRecipients());
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
