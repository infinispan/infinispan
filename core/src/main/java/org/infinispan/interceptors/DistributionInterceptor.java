package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.DataCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
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
import org.infinispan.remoting.ResponseMode;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.GlobalTransaction;
import org.infinispan.util.Immutables;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * // TODO: Document this
 *
 * @author manik
 * @since 4.0
 */
public class DistributionInterceptor extends BaseRpcInterceptor {
   DistributionManager dm;
   CommandsFactory cf;
   // TODO move this to the transaction context.  Will scale better there.
   private final Map<GlobalTransaction, List<Address>> txRecipients = new ConcurrentHashMap<GlobalTransaction, List<Address>>();
   static final RecipientGenerator CLEAR_COMMAND_GENERATOR = new RecipientGenerator() {

      public List<Address> generateRecipients() {
         return null;
      }

      public Object[] getKeys() {
         return null;
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
      if (returnValue == null) {
         // attempt a remote lookup
         // TODO update ClusteredGetCommand (maybe a new command?) to ensure we get back ICEs.
         ClusteredGetCommand get = cf.buildClusteredGetCommand(command.getKey());
         // TODO use a RspFilter to filter responses         
         List<Response> responses = rpcManager.invokeRemotely(dm.locate(command.getKey()), get, ResponseMode.SYNCHRONOUS,
                                                              configuration.getSyncReplTimeout(), false, false);

         // the first response is all that matters
         if (responses.isEmpty()) return returnValue;

         for (Object response : responses) {
            if (!(response instanceof Throwable)) {
               InternalCacheEntry ice = (InternalCacheEntry) response;
               if (configuration.isL1CacheEnabled()) {
                  long lifespan = ice.getLifespan() < 0 ? configuration.getL1Lifespan() : Math.min(ice.getLifespan(), configuration.getL1Lifespan());
                  PutKeyValueCommand put = cf.buildPutKeyValueCommand(ice.getKey(), ice.getValue(), lifespan, -1);
                  invokeNextInterceptor(ctx, put);
               }
               return ice.getValue();
            }
         }
         return null;
      }
      return returnValue;
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
      try {
         if (!skipReplicationOfTransactionMethod(ctx)) {
            List<Address> recipients = txRecipients.get(command.getGlobalTransaction());
            if (recipients != null) replicateCall(ctx, recipients, command, configuration.isSyncCommitPhase(), true);
         }
         return invokeNextInterceptor(ctx, command);
      } finally {
         txRecipients.remove(command.getGlobalTransaction());
      }
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

         List<Address> recipients = determineRecipients(command);
         txRecipients.put(command.getGlobalTransaction(), recipients);

         // this method will return immediately if we're the only member (because exclude_self=true)
         replicateCall(ctx, command, sync);
      }

      return retVal;
   }

   @Override
   public Object visitRollbackCommand(InvocationContext ctx, RollbackCommand command) throws Throwable {
      try {
         if (!skipReplicationOfTransactionMethod(ctx) && !ctx.isLocalRollbackOnly()) {
            List<Address> recipients = txRecipients.get(command.getGlobalTransaction());
            if (recipients != null) replicateCall(ctx, recipients, command, configuration.isSyncRollbackPhase(), true);
         }
         return invokeNextInterceptor(ctx, command);
      } finally {
         txRecipients.remove(command.getGlobalTransaction());
      }
   }

   private List<Address> determineRecipients(PrepareCommand cmd) {
      Set<Address> r = new HashSet<Address>();
      boolean toAll = false;
      for (WriteCommand c : cmd.getModifications()) {
         if (c instanceof ClearCommand) {
            toAll = true;
            break;
         } else {
            if (c instanceof DataCommand) {
               r.addAll(dm.locate(((DataCommand) c).getKey()));
            } else if (c instanceof PutMapCommand) {
               r.addAll(new MultipleKeysRecipientGenerator(((PutMapCommand) c).getMap().keySet()).generateRecipients());
            }
         }
      }

      return toAll ? null : Immutables.immutableListConvert(r);
   }


   /**
    * If we are within one transaction we won't do any replication as replication would only be performed at commit
    * time. If the operation didn't originate locally we won't do any replication either.
    */
   private Object handleWriteCommand(InvocationContext ctx, WriteCommand command, RecipientGenerator recipientGenerator) throws Throwable {
      boolean local = isLocalModeForced(ctx);
      if (local && ctx.getTransaction() == null) return invokeNextInterceptor(ctx, command);
      // FIRST pass this call up the chain.  Only if it succeeds (no exceptions) locally do we attempt to replicate.

      Object returnValue = invokeNextInterceptor(ctx, command);

      if (command.isSuccessful()) {
         if (ctx.getTransaction() == null && ctx.isOriginLocal()) {
            if (trace) {
               log.trace("invoking method " + command.getClass().getSimpleName() + ", members=" + rpcManager.getTransport().getMembers() + ", mode=" +
                     configuration.getCacheMode() + ", exclude_self=" + true + ", timeout=" +
                     configuration.getSyncReplTimeout());
            }

            List<Address> rec = recipientGenerator.generateRecipients();
            // if L1 caching is used make sure we broadcast an invalidate message
            if (configuration.isL1CacheEnabled() && rec != null) {
               InvalidateCommand ic = cf.buildInvalidateCommand(recipientGenerator.getKeys());
               replicateCall(ctx, ic, isSynchronous(ctx), false);
            }
            replicateCall(ctx, rec, command, isSynchronous(ctx), false);
         } else {
            if (local) ctx.getTransactionContext().addLocalModification(command);
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
