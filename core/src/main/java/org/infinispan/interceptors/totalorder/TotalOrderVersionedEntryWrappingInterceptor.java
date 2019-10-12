package org.infinispan.interceptors.totalorder;

import java.util.ArrayList;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.VersionedRepeatableReadEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.impl.VersionedEntryWrappingInterceptor;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Wrapping Interceptor for Total Order protocol when versions are needed
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @deprecated @deprecated since 10.0. Total Order will be removed.
 */
@Deprecated
public class TotalOrderVersionedEntryWrappingInterceptor extends VersionedEntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedEntryWrappingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final EntryVersionsMap EMPTY_VERSION_MAP = new EntryVersionsMap();

   private final InvocationSuccessFunction<VersionedPrepareCommand> prepareHandler = this::prepareHandler;
   private final InvocationSuccessFunction<VersionedPrepareCommand> afterPrepareHandler = this::afterPrepareHandler;

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      VersionedPrepareCommand versionedPrepareCommand = (VersionedPrepareCommand) command;
      if (ctx.isOriginLocal()) {
         versionedPrepareCommand.setVersionsSeen(ctx.getCacheTransaction().getVersionsRead());
         //for local mode keys
         ctx.getCacheTransaction().setUpdatedEntryVersions(EMPTY_VERSION_MAP);
         return invokeNextThenApply(ctx, command, (rCtx, rCommand, rv) -> {
            if (shouldCommitDuringPrepare(rCommand, (TxInvocationContext) rCtx)) {
               return delayedValue(commitContextEntries(rCtx, null), rv);
            }
            return rv;
         });
      }

      //Remote context, delivered in total order
      return wrapEntriesForPrepareAndApply(ctx, versionedPrepareCommand, prepareHandler);
   }

   private Object prepareHandler(InvocationContext ctx, VersionedPrepareCommand command, Object rv) {
      return invokeNextThenApply(ctx, command, afterPrepareHandler);
   }

   private Object afterPrepareHandler(InvocationContext ctx, VersionedPrepareCommand prepareCommand, Object rv) {
      TxInvocationContext txInvocationContext = (TxInvocationContext) ctx;
      CompletionStage<EntryVersionsMap> versionsMap =
            cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, txInvocationContext,
                  prepareCommand);

      InvocationStage versionStage = asyncValue(versionsMap);

      return versionStage.thenApply(ctx, prepareCommand, (rCtx, rCmd, rv2) -> {
         Object result = rv2 == null ? rv : new ArrayList<>(((EntryVersionsMap) rv2).keySet());
         if (prepareCommand.isOnePhaseCommit()) {
            return delayedValue(commitContextEntries(txInvocationContext, null), result);
         } else {
            if (trace)
               log.tracef("Transaction %s will be committed in the 2nd phase",
                     txInvocationContext.getGlobalTransaction().globalId());
         }
         return result;
      });
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextAndHandle(ctx, command, (rCtx, rCommand, rv, t) -> delayedValue(commitContextEntries(rCtx, null), rv, t));
   }

   @Override
   protected CompletionStage<Void> commitContextEntry(CacheEntry entry, InvocationContext ctx, FlagAffectedCommand command,
                                     Flag stateTransferFlag, boolean l1Invalidation) {
      if (ctx.isInTxScope() && stateTransferFlag == null) {
         // If user provided version, use it, otherwise generate/increment accordingly
         VersionedRepeatableReadEntry clusterMvccEntry = (VersionedRepeatableReadEntry) entry;
         EntryVersion existingVersion = clusterMvccEntry.getMetadata().version();
         EntryVersion newVersion;
         if (existingVersion == null) {
            newVersion = versionGenerator.generateNew();
         } else {
            newVersion = versionGenerator.increment((IncrementableEntryVersion) existingVersion);
         }

         entry.setMetadata(new EmbeddedMetadata.Builder().version(newVersion).build());
         return cdl.commitEntry(entry, command, ctx, null, l1Invalidation);
      } else {
         // This could be a state transfer call!
         return cdl.commitEntry(entry, command, ctx, stateTransferFlag, l1Invalidation);
      }
   }
}
