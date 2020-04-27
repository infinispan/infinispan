package org.infinispan.interceptors.impl;

import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.Ownership;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor for non-transactional caches to handle updates from remote sites.
 *
 * Remote sites only send {@link PutKeyValueCommand} or {@link RemoveCommand}.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class NonTxIracRemoteSiteInterceptor extends AbstractIracRemoteSiteInterceptor {

   private static final Log log = LogFactory.getLog(NonTxIracRemoteSiteInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   boolean isTraceEnabled() {
      return trace;
   }

   @Override
   Log getLog() {
      return log;
   }

   private Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      final Object key = command.getKey();
      if (isNormalWriteCommand(command)) {
         return invokeNext(ctx, command);
      }

      Ownership ownership = getOwnership(key);

      switch (ownership) {
         case PRIMARY:
            //we are on primary and the lock is acquired
            //if the update is discarded, command.isSuccessful() will return false.
            return invokeNextThenAccept(ctx, command, this::validateOnPrimary);
         case BACKUP:
            if (!ctx.isOriginLocal()) {
               //backups only commit when the command are remote (i.e. after validated from the originator)
               return invokeNextThenAccept(ctx, command, this::setIracMetadataForOwner);
            }
      }
      return invokeNext(ctx, command);
   }
}
