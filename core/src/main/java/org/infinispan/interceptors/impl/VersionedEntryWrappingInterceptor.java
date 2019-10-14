package org.infinispan.interceptors.impl;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
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
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.concurrent.CompletableFutures;
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

   private final InvocationSuccessFunction<VersionedPrepareCommand> prepareHandler = this::prepareHandler;

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      VersionedPrepareCommand versionedPrepareCommand = (VersionedPrepareCommand) command;
      if (ctx.isOriginLocal()) {
         versionedPrepareCommand.setVersionsSeen(ctx.getCacheTransaction().getVersionsRead());
      }
      return wrapEntriesForPrepareAndApply(ctx, versionedPrepareCommand, prepareHandler);
   }

   private Object prepareHandler(InvocationContext nonTxCtx, VersionedPrepareCommand command, Object nil) {
      TxInvocationContext ctx = (TxInvocationContext) nonTxCtx;
      CompletionStage<EntryVersionsMap> originVersionData;
      if (ctx.isOriginLocal() && !ctx.getCacheTransaction().isFromStateTransfer()) {
         originVersionData =
               cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx, command);
      } else {
         originVersionData = CompletableFutures.completedNull();
      }

      InvocationStage originVersionStage = makeStage(asyncInvokeNext(ctx, command, originVersionData));

      InvocationStage newVersionStage = originVersionStage.thenApplyMakeStage(ctx, command, (rCtx, rCommand, rv) -> {
         TxInvocationContext txInvocationContext = (TxInvocationContext) rCtx;
         VersionedPrepareCommand versionedPrepareCommand = (VersionedPrepareCommand) rCommand;
         if (txInvocationContext.isOriginLocal()) {
            // This is already completed, so just return a sync stage
            return asyncValue(originVersionData);
         } else {
            return asyncValue(cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, txInvocationContext,
                  versionedPrepareCommand));
         }
      });

      return newVersionStage.thenApply(ctx, command, (rCtx, rCommand, rv) -> {
         TxInvocationContext txInvocationContext = (TxInvocationContext) rCtx;
         VersionedPrepareCommand versionedPrepareCommand = (VersionedPrepareCommand) rCommand;
         boolean onePhaseCommit = versionedPrepareCommand.isOnePhaseCommit();
         if (onePhaseCommit) {
            txInvocationContext.getCacheTransaction()
                  .setUpdatedEntryVersions(versionedPrepareCommand.getVersionsSeen());
         }

         CompletionStage<Void> stage = null;
         if (onePhaseCommit) {
            stage = commitContextEntries(txInvocationContext, null);
         }
         return delayedValue(stage, rv);
      });
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      VersionedCommitCommand versionedCommitCommand = (VersionedCommitCommand) command;
      if (ctx.isOriginLocal()) {
         versionedCommitCommand.setUpdatedVersions(ctx.getCacheTransaction().getUpdatedEntryVersions());
      }

      return invokeNextAndHandle(ctx, versionedCommitCommand, (rCtx, rCommand, rv, t) ->
            delayedValue(doCommit(rCtx, rCommand), rv, t));
   }

   private CompletionStage<Void> doCommit(InvocationContext rCtx, VersionedCommitCommand versionedCommitCommand) {
      if (!rCtx.isOriginLocal()) {
         ((TxInvocationContext<?>) rCtx).getCacheTransaction().setUpdatedEntryVersions(
               versionedCommitCommand.getUpdatedVersions());
      }
      return commitContextEntries(rCtx, null);
   }

   @Override
   protected CompletionStage<Void> commitContextEntry(CacheEntry entry, InvocationContext ctx, FlagAffectedCommand command,
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
         return cdl.commitEntry(entry, command, ctx, null, l1Invalidation);
      } else {
         // This could be a state transfer call!
         return cdl.commitEntry(entry, command, ctx, stateTransferFlag, l1Invalidation);
      }
   }

}
