package org.infinispan.interceptors.impl;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor in charge with wrapping entries and add them in caller's context.
 *
 * @author Mircea Markus
 * @since 9.0
 */
public class VersionedEntryWrappingInterceptor extends EntryWrappingInterceptor {
   private static final Log log = LogFactory.getLog(VersionedEntryWrappingInterceptor.class);

   @Inject protected VersionGenerator versionGenerator;

   private final InvocationSuccessFunction prepareHandler = this::prepareHandler;

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      VersionedPrepareCommand versionedPrepareCommand = (VersionedPrepareCommand) command;
      if (ctx.isOriginLocal()) {
         versionedPrepareCommand.setVersionsSeen(ctx.getCacheTransaction().getVersionsRead());
      }
      return wrapEntriesForPrepareAndApply(ctx, command, prepareHandler);
   }

   private Object prepareHandler(InvocationContext nonTxCtx, VisitableCommand command, Object nil) {
      TxInvocationContext ctx = (TxInvocationContext) nonTxCtx;
      EntryVersionsMap originVersionData;
      if (ctx.isOriginLocal() && !ctx.getCacheTransaction().isFromStateTransfer()) {
         originVersionData =
               cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx, (VersionedPrepareCommand) command);
      } else {
         originVersionData = null;
      }

      return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
         TxInvocationContext txInvocationContext = (TxInvocationContext) rCtx;
         VersionedPrepareCommand versionedPrepareCommand = (VersionedPrepareCommand) rCommand;
         EntryVersionsMap newVersionData;
         if (txInvocationContext.isOriginLocal()) {
            newVersionData = originVersionData;
         } else {
            newVersionData = cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, txInvocationContext,
                  versionedPrepareCommand);
         }

         boolean onePhaseCommit = ((PrepareCommand) rCommand).isOnePhaseCommit();
         if (onePhaseCommit) {
            txInvocationContext.getCacheTransaction()
                  .setUpdatedEntryVersions(versionedPrepareCommand.getVersionsSeen());
         }

         if (onePhaseCommit) {
            commitContextEntries(txInvocationContext, null);
         }
         return newVersionData;
      });
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      VersionedCommitCommand versionedCommitCommand = (VersionedCommitCommand) command;
      if (ctx.isOriginLocal()) {
         versionedCommitCommand.setUpdatedVersions(ctx.getCacheTransaction().getUpdatedEntryVersions());
      }

      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) ->
            doCommit(rCtx, ((VersionedCommitCommand) rCommand)));
   }

   private void doCommit(InvocationContext rCtx, VersionedCommitCommand versionedCommitCommand) {
      if (!rCtx.isOriginLocal()) {
         ((TxInvocationContext<?>) rCtx).getCacheTransaction().setUpdatedEntryVersions(
               versionedCommitCommand.getUpdatedVersions());
      }
      commitContextEntries(rCtx, null);
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, FlagAffectedCommand command,
                                     Flag stateTransferFlag, boolean l1Invalidation) {
      if (ctx.isInTxScope() && stateTransferFlag == null) {
         EntryVersion updatedEntryVersion = ((TxInvocationContext) ctx)
               .getCacheTransaction().getUpdatedEntryVersions().get(entry.getKey());
         Metadata commitMetadata;
         if (updatedEntryVersion != null) {
            if (entry.getMetadata() == null) {
               commitMetadata = new EmbeddedMetadata.Builder().version(updatedEntryVersion).build();
            } else {
               commitMetadata = entry.getMetadata().builder().version(updatedEntryVersion).build();
            }
         } else {
            commitMetadata = entry.getMetadata();
         }

         entry.setMetadata(commitMetadata);
         cdl.commitEntry(entry, command, ctx, null, l1Invalidation);
      } else {
         // This could be a state transfer call!
         cdl.commitEntry(entry, command, ctx, stateTransferFlag, l1Invalidation);
      }
   }

}
