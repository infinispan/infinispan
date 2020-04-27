package org.infinispan.interceptors.impl;

import java.util.List;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.Flag;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.infinispan.xsite.irac.DiscardUpdateException;

/**
 * Interceptor for optimistic transactional caches to handle updates from remote sites.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class OptimisticTxIracRemoteSiteInterceptor extends AbstractIracRemoteSiteInterceptor {

   private static final Log log = LogFactory.getLog(OptimisticTxIracRemoteSiteInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   /**
    * @return {@code true} if it is a normal transaction. An update from remote site is a single {@link WriteCommand}
    * with a {@link Flag#IRAC_UPDATE} set.
    */
   private static boolean isNormalTransaction(List<WriteCommand> mods) {
      return mods.size() != 1 || isNormalWriteCommand(mods.get(0));
   }

   /**
    * @return {@code true} if it is a normal transaction. An update from remote site is a single {@link WriteCommand}
    * with a {@link Flag#IRAC_UPDATE} set.
    */
   private static boolean isNormalTransaction(WriteCommand[] mods) {
      return mods.length != 1 || isNormalWriteCommand(mods[0]);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      if (isNormalTransaction(command.getModifications())) {
         return invokeNext(ctx, command);
      }
      DataWriteCommand cmd = (DataWriteCommand) command.getModifications()[0];
      if (getDistributionInfo(cmd.getKey()).isPrimary()) {
         validateOnPrimary(ctx, cmd, null);
         //this works for now.
         //but when we introduce the custom conflict resolution a new version is generated and we need to send it back
         //to the originator so it can broadcast it everywhere
         if (!cmd.isSuccessful()) {
            throw DiscardUpdateException.getInstance();
         }
      }
      return invokeNext(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      //noinspection unchecked
      if (isNormalTransaction(ctx.getModifications())) {
         return invokeNext(ctx, command);
      }
      // work for now. see comment in prepare
      setIracMetadataForOwner(ctx, (DataWriteCommand) ctx.getModifications().get(0), null);
      return invokeNext(ctx, command);
   }

   @Override
   boolean isTraceEnabled() {
      return trace;
   }

   @Override
   Log getLog() {
      return log;
   }
}
