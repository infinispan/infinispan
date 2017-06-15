package org.infinispan.interceptors.xsite;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
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
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

   private Object handleSingleKeyWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
         DataWriteCommand dataWriteCommand = (DataWriteCommand) rCommand;
         if (!skipXSiteBackup(dataWriteCommand)) {
            if (dataWriteCommand.isSuccessful() &&
                  clusteringDependentLogic.getCacheTopology().getDistribution(dataWriteCommand.getKey()).isPrimary()) {
               backupSender.processResponses(backupSender.backupWrite(transform(dataWriteCommand)), dataWriteCommand);
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
      } else if (command instanceof ComputeCommand) {
         ComputeCommand computeCommand = (ComputeCommand) command;
         return commandsFactory.buildComputeCommand(computeCommand.getKey(), computeCommand.getRemappingBiFunction(), computeCommand.isComputeIfPresent(), computeCommand.getMetadata(), computeCommand.getFlagsBitSet());
      } else if (command instanceof ComputeIfAbsentCommand) {
         ComputeIfAbsentCommand computeIfAbsentCommand = (ComputeIfAbsentCommand) command;
         return commandsFactory.buildComputeIfAbsentCommand(computeIfAbsentCommand.getKey(), computeIfAbsentCommand.getMappingFunction(), computeIfAbsentCommand.getMetadata(), computeIfAbsentCommand.getFlagsBitSet());
      }
      throw new IllegalArgumentException("Command " + command + " is not valid!");
   }
}
