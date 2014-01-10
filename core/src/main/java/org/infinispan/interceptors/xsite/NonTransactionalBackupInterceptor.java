package org.infinispan.interceptors.xsite;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.write.ClearCommand;
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
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

   private Object handleSingleKeyWriteCommand(InvocationContext ctx, DataWriteCommand command) throws Throwable {
      Object result = invokeNextInterceptor(ctx, command);
      if (skipXSiteBackup(command)) {
         return result;
      } else if (command.isSuccessful() && clusteringDependentLogic.localNodeIsPrimaryOwner(command.getKey())) {
         backupSender.processResponses(backupSender.backupWrite(transform(command)), command);
      }
      return result;
   }

   private Object handleMultipleKeysWriteCommand(InvocationContext ctx, WriteCommand command) throws Throwable {
      if (!ctx.isOriginLocal() || skipXSiteBackup(command)) {
         return invokeNextInterceptor(ctx, command);
      }
      Object result = invokeNextInterceptor(ctx, command);
      backupSender.processResponses(backupSender.backupWrite(command), command);
      return result;
   }

   private WriteCommand transform(DataWriteCommand command) {
      if (command instanceof PutKeyValueCommand) {
         PutKeyValueCommand putCommand = (PutKeyValueCommand) command;
         return commandsFactory.buildPutKeyValueCommand(putCommand.getKey(), putCommand.getValue(),
                                                        command.getMetadata(), command.getFlags());
      } else if (command instanceof ReplaceCommand) {
         ReplaceCommand replaceCommand = (ReplaceCommand) command;
         return commandsFactory.buildPutKeyValueCommand(replaceCommand.getKey(), replaceCommand.getNewValue(),
                                                        command.getMetadata(), command.getFlags());
      } else if (command instanceof RemoveCommand) {
         return commandsFactory.buildRemoveCommand(command.getKey(), null, command.getFlags());
      }
      throw new IllegalArgumentException("Command " + command + " is not valid!");
   }
}
