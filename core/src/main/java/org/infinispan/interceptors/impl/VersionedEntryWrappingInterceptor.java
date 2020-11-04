package org.infinispan.interceptors.impl;

import static org.infinispan.remoting.responses.PrepareResponse.asPrepareResponse;
import static org.infinispan.transaction.impl.WriteSkewHelper.mergeInPrepareResponse;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InvocationFinallyFunction;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.metadata.impl.PrivateMetadata;
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
   private final boolean trace = log.isTraceEnabled();

   @Inject protected VersionGenerator versionGenerator;

   private final InvocationSuccessFunction<VersionedPrepareCommand> prepareHandler = this::prepareHandler;
   private final InvocationSuccessFunction<VersionedPrepareCommand> afterPrepareHandler = this::afterPrepareHandler;
   private final InvocationFinallyFunction<VersionedCommitCommand> commitHandler = this::commitHandler;

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      VersionedPrepareCommand versionedPrepareCommand = (VersionedPrepareCommand) command;
      if (ctx.isOriginLocal()) {
         versionedPrepareCommand.setVersionsSeen(ctx.getCacheTransaction().getVersionsRead());
      }
      return wrapEntriesForPrepareAndApply(ctx, versionedPrepareCommand, prepareHandler);
   }

   private Object prepareHandler(InvocationContext nonTxCtx, VersionedPrepareCommand command, Object nil) {
      TxInvocationContext<?> ctx = (TxInvocationContext<?>) nonTxCtx;
      CompletionStage<Map<Object, IncrementableEntryVersion>> originVersionData;
      if (ctx.getCacheTransaction().isFromStateTransfer()) {
         storeEntryVersionForStateTransfer(ctx);
         originVersionData = CompletableFutures.completedNull();
      } else if (ctx.isOriginLocal()) {
         originVersionData = checkWriteSkew(ctx, command);
      } else {
         originVersionData = CompletableFutures.completedNull();
      }

      InvocationStage originVersionStage = makeStage(asyncInvokeNext(ctx, command, originVersionData));

      InvocationStage newVersionStage = originVersionStage.thenApplyMakeStage(ctx, command, (rCtx, rCommand, rv) -> {
         CompletionStage<Map<Object, IncrementableEntryVersion>> stage = rCtx.isOriginLocal() ?
               originVersionData :
               checkWriteSkew((TxInvocationContext<?>) rCtx, rCommand);
         return asyncValue(stage.thenApply(vMap -> mergeInPrepareResponse(vMap, asPrepareResponse(rv))));
      });

      return newVersionStage.thenApply(ctx, command, afterPrepareHandler);
   }

   private Object afterPrepareHandler(InvocationContext ctx, VersionedPrepareCommand command, Object rv) {
      if (command.isOnePhaseCommit()) {
         TxInvocationContext<?> txCtx = (TxInvocationContext<?>) ctx;
         txCtx.getCacheTransaction().setUpdatedEntryVersions(command.getVersionsSeen());
         CompletionStage<Void> stage = commitContextEntries(ctx, null);
         return delayedValue(stage, rv);
      }
      return rv;
   }

   @SuppressWarnings("rawtypes")
   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      VersionedCommitCommand versionedCommitCommand = (VersionedCommitCommand) command;
      if (ctx.isOriginLocal()) {
         versionedCommitCommand.setUpdatedVersions(ctx.getCacheTransaction().getUpdatedEntryVersions());
      }

      return invokeNextAndHandle(ctx, versionedCommitCommand, commitHandler);
   }

   @Override
   protected CompletionStage<Void> commitContextEntry(CacheEntry<?, ?> entry, InvocationContext ctx,
         FlagAffectedCommand command, Flag stateTransferFlag, boolean l1Invalidation) {
      if (ctx.isInTxScope() && stateTransferFlag == null) {
         storeEntryVersion(entry, (TxInvocationContext<?>) ctx);
      }
      return cdl.commitEntry(entry, command, ctx, stateTransferFlag, l1Invalidation);
   }

   private void storeEntryVersion(CacheEntry<?, ?> entry, TxInvocationContext<?> ctx) {
      IncrementableEntryVersion entryVersion = ctx.getCacheTransaction().getUpdatedEntryVersions().get(entry.getKey());
      if (entryVersion == null) {
         return; //nothing to set
      }
      PrivateMetadata.Builder builder = PrivateMetadata.getBuilder(entry.getInternalMetadata());
      builder.entryVersion(entryVersion);
      entry.setInternalMetadata(builder.build());
   }

   private void storeEntryVersionForStateTransfer(TxInvocationContext<?> ctx) {
      //the write command has the PrivateMetadata with the version when it is received from other nodes (state transfer).
      //we need to set copy the PrivateMetadata to the contxt entry.
      for (WriteCommand cmd : ctx.getCacheTransaction().getAllModifications()) {
         for (Object key : cmd.getAffectedKeys()) {
            PrivateMetadata metadata = cmd.getInternalMetadata(key);
            assert metadata != null;
            IncrementableEntryVersion entryVersion = metadata.entryVersion();
            assert entryVersion != null;
            CacheEntry<?, ?> entry = ctx.lookupEntry(key);
            PrivateMetadata.Builder builder = PrivateMetadata.getBuilder(entry.getInternalMetadata());
            entry.setInternalMetadata(builder.entryVersion(entryVersion).build());
            if (trace) {
               log.tracef("Updated entry from state transfer: %s", entry);
            }
         }
      }
   }

   private Object commitHandler(InvocationContext ctx, VersionedCommitCommand command, Object rv, Throwable t) {
      return delayedValue(doCommit(ctx, command), rv, t);
   }

   private CompletionStage<Map<Object, IncrementableEntryVersion>> checkWriteSkew(TxInvocationContext<?> ctx,
         VersionedPrepareCommand command) {
      return cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx, command);
   }

   private CompletionStage<Void> doCommit(InvocationContext ctx, VersionedCommitCommand command) {
      if (!ctx.isOriginLocal()) {
         ((TxInvocationContext<?>) ctx).getCacheTransaction().setUpdatedEntryVersions(command.getUpdatedVersions());
      }
      return commitContextEntries(ctx, null);
   }

}
