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
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;

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
   public InvocationStage visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public InvocationStage visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

   private InvocationStage handleSingleKeyWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      return invokeNext(ctx, command).thenAccept(command, (rCommand, rv) -> {
         if (!skipXSiteBackup(rCommand)) {
            if (rCommand.isSuccessful() && clusteringDependentLogic.localNodeIsPrimaryOwner(rCommand.getKey())) {
               try {
                  backupSender.processResponses(backupSender.backupWrite(transform(rCommand)), rCommand);
               } catch (Throwable t) {
                  rethrowAsCompletedException(t);
               }
            }
         }
      });
   }

   private WriteCommand transform(DataWriteCommand command) {
      if (command instanceof PutKeyValueCommand) {
         PutKeyValueCommand putCommand = (PutKeyValueCommand) command;
         return commandsFactory.buildPutKeyValueCommand(putCommand.getKey(), putCommand.getValue(),
                                                        putCommand.getMetadata(), putCommand.getFlagsBitSet());
      } else if (command instanceof ReplaceCommand) {
         ReplaceCommand replaceCommand = (ReplaceCommand) command;
         return commandsFactory.buildPutKeyValueCommand(replaceCommand.getKey(), replaceCommand.getNewValue(),
                                                        replaceCommand.getMetadata(), replaceCommand.getFlagsBitSet());
      } else if (command instanceof RemoveCommand) {
         return commandsFactory.buildRemoveCommand(command.getKey(), null, command.getFlagsBitSet());
      }
      throw new IllegalArgumentException("Command " + command + " is not valid!");
   }
}
