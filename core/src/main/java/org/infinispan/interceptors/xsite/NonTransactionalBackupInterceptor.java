package org.infinispan.interceptors.xsite;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

import java.util.concurrent.CompletableFuture;

/**
 * Handles x-site data backups for non-transactional caches.
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NonTransactionalBackupInterceptor extends BaseBackupInterceptor {

   private CommandsFactory commandsFactory;
   private ClusteringDependentLogic clusteringDependentLogic;

   @Inject
   public void injectDependencies(CommandsFactory commandsFactory, ClusteringDependentLogic clusteringDependentLogic) {
      this.commandsFactory = commandsFactory;
      this.clusteringDependentLogic = clusteringDependentLogic;
   }

   @Override
   public CompletableFuture<Void> visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public CompletableFuture<Void> visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

   private CompletableFuture<Void> handleSingleKeyWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      Object result = ctx.forkInvocationSync(command);
      if (skipXSiteBackup(command)) {
         return ctx.shortCircuit(result);
      } else if (command.isSuccessful() && clusteringDependentLogic.localNodeIsPrimaryOwner(command.getKey())) {
         backupSender.processResponses(backupSender.backupWrite(transform(command)), command);
      }
      return ctx.shortCircuit(result);
   }

   private WriteCommand transform(DataWriteCommand command) {
      if (command instanceof PutKeyValueCommand) {
         PutKeyValueCommand putCommand = (PutKeyValueCommand) command;
         return commandsFactory.buildPutKeyValueCommand(putCommand.getKey(), putCommand.getValue(),
                                                        command.getMetadata(), command.getFlagsBitSet());
      } else if (command instanceof ReplaceCommand) {
         ReplaceCommand replaceCommand = (ReplaceCommand) command;
         return commandsFactory.buildPutKeyValueCommand(replaceCommand.getKey(), replaceCommand.getNewValue(),
                                                        command.getMetadata(), command.getFlagsBitSet());
      } else if (command instanceof RemoveCommand) {
         return commandsFactory.buildRemoveCommand(command.getKey(), null, command.getFlagsBitSet());
      }
      throw new IllegalArgumentException("Command " + command + " is not valid!");
   }
}
