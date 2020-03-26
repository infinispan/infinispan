package org.infinispan.interceptors.impl;

import java.util.Map;
import static org.infinispan.remoting.responses.PrepareResponse.asPrepareResponse;
import static org.infinispan.transaction.impl.WriteSkewHelper.mergeInPrepareResponse;

import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
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
      CompletionStage<Map<Object, IncrementableEntryVersion>> originVersionData;
      if (ctx.isOriginLocal() && !ctx.getCacheTransaction().isFromStateTransfer()) {
         originVersionData = cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx, command);
      } else {
         originVersionData = CompletableFutures.completedNull();
      }

      InvocationStage originVersionStage = makeStage(asyncInvokeNext(ctx, command, originVersionData));

      InvocationStage newVersionStage = originVersionStage.thenApplyMakeStage(ctx, command, (rCtx, rCommand, rv) -> {
         TxInvocationContext txCtx = (TxInvocationContext) rCtx;
         if (txCtx.isOriginLocal()) {
            // This is already completed, so just return a sync stage
            return asyncValue(originVersionData
                  .thenApply(versionsMap -> mergeInPrepareResponse(versionsMap, asPrepareResponse(rv))));
         } else {
            return asyncValue(cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, txCtx, rCommand)
                  .thenApply(versionsMap -> mergeInPrepareResponse(versionsMap, asPrepareResponse(rv))));
         }
      });

      return newVersionStage.thenApply(ctx, command, (rCtx, rCommand, rv) -> {
         TxInvocationContext txInvocationContext = (TxInvocationContext) rCtx;
         boolean onePhaseCommit = rCommand.isOnePhaseCommit();
         if (onePhaseCommit) {
            txInvocationContext.getCacheTransaction().setUpdatedEntryVersions(rCommand.getVersionsSeen());
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
         Metadata commitMetadata = createMetadataForCommit(entry, updatedEntryVersion);
         entry.setMetadata(commitMetadata);
         return cdl.commitEntry(entry, command, ctx, null, l1Invalidation);
      } else {
         // This could be a state transfer call!
         return cdl.commitEntry(entry, command, ctx, stateTransferFlag, l1Invalidation);
      }
   }

   protected Metadata createMetadataForCommit(CacheEntry<?, ?> entry, EntryVersion version) {
      if (version != null) {
         if (entry.getMetadata() == null) {
            return new EmbeddedMetadata.Builder().version(version).build();
         } else {
            return entry.getMetadata().builder().version(version).build();
         }
      } else {
         return entry.getMetadata();
      }
   }

}
