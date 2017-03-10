package org.infinispan.interceptors.totalorder;

import java.util.ArrayList;

import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.VisitableCommand;
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
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.impl.VersionedEntryWrappingInterceptor;
import org.infinispan.metadata.Metadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Wrapping Interceptor for Total Order protocol when versions are needed
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 */
public class TotalOrderVersionedEntryWrappingInterceptor extends VersionedEntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedEntryWrappingInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final EntryVersionsMap EMPTY_VERSION_MAP = new EntryVersionsMap();

   private final InvocationSuccessFunction prepareHandler = this::prepareHandler;
   private final InvocationSuccessFunction afterPrepareHandler = this::afterPrepareHandler;

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         ((VersionedPrepareCommand) command).setVersionsSeen(ctx.getCacheTransaction().getVersionsRead());
         //for local mode keys
         ctx.getCacheTransaction().setUpdatedEntryVersions(EMPTY_VERSION_MAP);
         return invokeNextThenAccept(ctx, command, (rCtx, rCommand, rv) -> {
            if (shouldCommitDuringPrepare((PrepareCommand) rCommand, ctx)) {
               commitContextEntries(ctx, null);
            }
         });
      }

      //Remote context, delivered in total order
      return wrapEntriesForPrepareAndApply(ctx, command, prepareHandler);
   }

   private Object prepareHandler(InvocationContext ctx, VisitableCommand command, Object rv) {
      return invokeNextThenApply(ctx, command, afterPrepareHandler);
   }

   private Object afterPrepareHandler(InvocationContext ctx, VisitableCommand command, Object rv) {
      TxInvocationContext txInvocationContext = (TxInvocationContext) ctx;
      VersionedPrepareCommand prepareCommand = (VersionedPrepareCommand) command;
      EntryVersionsMap versionsMap =
            cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, txInvocationContext,
                  prepareCommand);

      if (prepareCommand.isOnePhaseCommit()) {
         commitContextEntries(txInvocationContext, null);
      } else {
         if (trace)
            log.tracef("Transaction %s will be committed in the 2nd phase",
                  txInvocationContext.getGlobalTransaction().globalId());
      }

      return versionsMap == null ? rv : new ArrayList<>(versionsMap.keySet());
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      return invokeNextAndFinally(ctx, command, (rCtx, rCommand, rv, t) -> commitContextEntries(rCtx, null));
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, FlagAffectedCommand command,
                                     Flag stateTransferFlag, boolean l1Invalidation) {
      if (ctx.isInTxScope() && stateTransferFlag == null) {
         // If user provided version, use it, otherwise generate/increment accordingly
         VersionedRepeatableReadEntry clusterMvccEntry = (VersionedRepeatableReadEntry) entry;
         Metadata metadata = clusterMvccEntry.getMetadata();
         EntryVersion existingVersion = metadata.version();
         EntryVersion newVersion;
         if (entry.isRemoved()) {
            newVersion = versionGenerator.nonExistingVersion();
         } else if (existingVersion == null) {
            newVersion = versionGenerator.generateNew();
         } else {
            newVersion = versionGenerator.increment((IncrementableEntryVersion) existingVersion);
         }

         entry.setMetadata(metadata.builder().version(newVersion).build());
         cdl.commitEntry(entry, command, ctx, null, l1Invalidation);
      } else {
         // This could be a state transfer call!
         cdl.commitEntry(entry, command, ctx, stateTransferFlag, l1Invalidation);
      }
   }
}
