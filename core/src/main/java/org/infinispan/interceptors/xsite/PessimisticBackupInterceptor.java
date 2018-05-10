package org.infinispan.interceptors.xsite;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.transport.BackupResponse;

/**
 * Handles x-site data backups for pessimistic transactional caches.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class PessimisticBackupInterceptor extends BaseBackupInterceptor {

   @Inject private ComponentRegistry componentRegistry;
   @Inject protected DataContainer<Object, Object> dataContainer;

   private final InvocationSuccessFunction handleLocalGetReturn = this::handleLocalGetReturn;

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      //for pessimistic transaction we don't do a 2PC (as we already own the remote lock) but just
      //a 1PC
      throw new IllegalStateException("This should never happen!");
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      final String cacheName = componentRegistry.getCacheName();

      // TODO temporary until protobuf metadata interceptor is in place
      if (cacheName.equals("___protobuf_metadata")) {
         return invokeNextThenApply(ctx, command, handleLocalGetReturn);
      } else {
         return invokeNext(ctx, command);
      }
   }

   private Object handleLocalGetReturn(InvocationContext ctx, VisitableCommand rCommand, Object rv) throws Throwable {
      if (rv == null) {
         final GetKeyValueCommand getCmd = (GetKeyValueCommand) rCommand;
         BackupResponse backupResponse =
            backupSender.backupGet(getCmd);

         backupSender.processResponses(backupResponse, rCommand);
         final Object value = getValue(backupResponse);

         if (value != null)
            dataContainer.compute(getCmd.getKey(),
               (k, oldEntry, factory) ->
                  factory.create(k, value, (Metadata) null)
            );

         return value;
      }

      return rv;
   }

   private Object getValue(BackupResponse backupResponse) {
      return backupResponse.getValues().values().stream()
         .findFirst()
         .orElse(null);
   }

}
