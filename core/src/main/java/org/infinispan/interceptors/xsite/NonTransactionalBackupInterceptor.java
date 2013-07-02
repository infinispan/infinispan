package org.infinispan.interceptors.xsite;

import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.remoting.transport.BackupResponse;

/**
 * Handles x-site data backups for non-transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class NonTransactionalBackupInterceptor extends BaseBackupInterceptor {

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleWrite(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleWrite(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleWrite(ctx, command);
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      return handleWrite(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleWrite(ctx, command);
   }

   private Object handleWrite(InvocationContext ctx, WriteCommand command) throws Throwable {
      if (!ctx.isOriginLocal() || command.hasFlag(Flag.SKIP_XSITE_BACKUP))
         return invokeNextInterceptor(ctx, command);
      BackupResponse backupResponse = backupSender.backupWrite(command);
      Object result = invokeNextInterceptor(ctx, command);
      backupSender.processResponses(backupResponse, command);
      return result;
   }
}
