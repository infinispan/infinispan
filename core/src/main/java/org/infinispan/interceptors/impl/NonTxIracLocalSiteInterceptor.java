package org.infinispan.interceptors.impl;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.RemoveExpiredCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.Ownership;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor used by IRAC for non transactional caches to handle the local site updates.
 * <p>
 * The primary owner job is to generate a new {@link IracMetadata} for the write and store in the {@link WriteCommand}.
 * If the command is successful, the {@link IracMetadata} is stored in the context entry.
 * <p>
 * The backup owners just handle the updates from the primary owner and extract the {@link IracMetadata} to stored in
 * the context entry.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class NonTxIracLocalSiteInterceptor extends AbstractIracLocalSiteInterceptor {

   private static final Log log = LogFactory.getLog(NonTxIracLocalSiteInterceptor.class);
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
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) {
      throw new UnsupportedOperationException();
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) {
      return visitDataWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) {
      return visitWriteCommand(ctx, command);
   }

   @Override
   public boolean isTraceEnabled() {
      return trace;
   }

   @Override
   public Log getLog() {
      return log;
   }

   private Object visitDataWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      final Object key = command.getKey();
      if (isIracState(command)) { //all the state transfer/preload is done via put commands.
         setMetadataToCacheEntry(ctx.lookupEntry(key), command.getInternalMetadata(key).iracMetadata());
         return invokeNext(ctx, command);
      }
      if (skipCommand(ctx, command)) {
         return invokeNext(ctx, command);
      }
      visitKey(ctx, key, command);
      return invokeNextAndFinally(ctx, command, this::handleDataWriteCommand);
   }

   private Object visitWriteCommand(InvocationContext ctx, WriteCommand command) {
      if (skipCommand(ctx, command)) {
         return invokeNext(ctx, command);
      }
      for (Object key : command.getAffectedKeys()) {
         visitKey(ctx, key, command);
      }
      return invokeNextAndFinally(ctx, command, this::handleWriteCommand);
   }

   private boolean skipCommand(InvocationContext ctx, FlagAffectedCommand command) {
      return ctx.isInTxScope() || command.hasAnyFlag(FlagBitSets.IRAC_UPDATE);
   }

   /**
    * Visits the {@link WriteCommand} before executing it.
    * <p>
    * The primary owner generates a new {@link IracMetadata} and stores it in the {@link WriteCommand}.
    */
   private void visitKey(InvocationContext ctx, Object key, WriteCommand command) {
      int segment = getSegment(command, key);
      if (getOwnership(segment) != Ownership.PRIMARY) {
         return;
      }
      IracMetadata metadata = null;
      // RemoveExpired should lose to any other conflicting write
      if (command instanceof RemoveExpiredCommand) {
         CacheEntry<?, ?> ce = ctx.lookupEntry(key);
         PrivateMetadata pm = ce.getInternalMetadata();
         if (pm != null) {
            metadata = pm.iracMetadata();
         }

         if (metadata == null) {
            metadata = iracVersionGenerator.generateMetadataWithCurrentVersion(segment);
         }
      } else {
         metadata = iracVersionGenerator.generateNewMetadata(segment);
      }
      updateCommandMetadata(key, command, metadata);
      if (trace) {
         log.tracef("[IRAC] New metadata for key '%s' is %s. Command=%s", key, metadata, command);
      }
   }

   /**
    * Visits th {@link WriteCommand} after executed and stores the {@link IracMetadata} if it was successful.
    */
   private void handleDataWriteCommand(InvocationContext ctx, DataWriteCommand command, Object rv, Throwable t) {
      final Object key = command.getKey();
      if (!command.isSuccessful() || skipEntryCommit(ctx, key)) {
         return;
      }
      setMetadataToCacheEntry(ctx.lookupEntry(key), command.getInternalMetadata(key).iracMetadata());
   }

   /**
    * Visits th {@link WriteCommand} after executed and stores the {@link IracMetadata} if it was successful.
    */
   private void handleWriteCommand(InvocationContext ctx, WriteCommand command, Object rv, Throwable t) {
      if (!command.isSuccessful()) {
         return;
      }
      for (Object key : command.getAffectedKeys()) {
         if (skipEntryCommit(ctx, key)) {
            continue;
         }
         setMetadataToCacheEntry(ctx.lookupEntry(key), command.getInternalMetadata(key).iracMetadata());
      }
   }

   private boolean skipEntryCommit(InvocationContext ctx, Object key) {
      switch (getOwnership(key)) {
         case NON_OWNER:
            //not a write owner, we do nothing
            return true;
         case BACKUP:
            //if it is local, we do nothing.
            //the update happens in the remote context after the primary validated the write
            if (ctx.isOriginLocal()) {
               return true;
            }
      }
      return false;
   }
}
