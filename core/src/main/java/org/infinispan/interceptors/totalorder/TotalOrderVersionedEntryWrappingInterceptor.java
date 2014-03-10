package org.infinispan.interceptors.totalorder;

import org.infinispan.context.Flag;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClusteredRepeatableReadEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.VersionedEntryWrappingInterceptor;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;

/**
 * Wrapping Interceptor for Total Order protocol when versions are needed
 *
 * @author Mircea.Markus@jboss.com
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderVersionedEntryWrappingInterceptor extends VersionedEntryWrappingInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderVersionedEntryWrappingInterceptor.class);
   private static final EntryVersionsMap EMPTY_VERSION_MAP = new EntryVersionsMap();
   private boolean trace;

   @Start
   public void setLogLevel() {
      trace = log.isTraceEnabled();
   }

   @Override
   public final Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {

      if (ctx.isOriginLocal()) {
         ((VersionedPrepareCommand) command).setVersionsSeen(ctx.getCacheTransaction().getVersionsRead());
         //for local mode keys
         ctx.getCacheTransaction().setUpdatedEntryVersions(EMPTY_VERSION_MAP);
         Object retVal = invokeNextInterceptor(ctx, command);
         if (shouldCommitDuringPrepare(command, ctx)) {
            commitContextEntries(ctx, null, null);
         }
         return retVal;
      }

      //Remote context, delivered in total order

      wrapEntriesForPrepare(ctx, command);

      Object retVal = invokeNextInterceptor(ctx, command);

      EntryVersionsMap versionsMap = cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx,
                                                                                (VersionedPrepareCommand) command);

      if (command.isOnePhaseCommit()) {
         commitContextEntries(ctx, null, null);
      } else {
         if (trace)
            log.tracef("Transaction %s will be committed in the 2nd phase", ctx.getGlobalTransaction().globalId());
      }

      return versionsMap == null ? retVal : new ArrayList<Object>(versionsMap.keySet());
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         return invokeNextInterceptor(ctx, command);
      } finally {
         commitContextEntries(ctx, null, null);
      }
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, FlagAffectedCommand command,
                                     Metadata metadata, Flag stateTransferFlag, boolean l1Invalidation) {
      if (ctx.isInTxScope() && stateTransferFlag == null) {
         Metadata commitMetadata;
         // If user provided version, use it, otherwise generate/increment accordingly
         ClusteredRepeatableReadEntry clusterMvccEntry = (ClusteredRepeatableReadEntry) entry;
         EntryVersion existingVersion = clusterMvccEntry.getMetadata().version();
         EntryVersion newVersion;
         if (existingVersion == null) {
            newVersion = versionGenerator.generateNew();
         } else {
            newVersion = versionGenerator.increment((IncrementableEntryVersion) existingVersion);
         }

         if (metadata == null)
            commitMetadata = new EmbeddedMetadata.Builder().version(newVersion).build();
         else
            commitMetadata = metadata.builder().version(newVersion).build();

         cdl.commitEntry(entry, commitMetadata, command, ctx, null, l1Invalidation);
      } else {
         // This could be a state transfer call!
         cdl.commitEntry(entry, entry.getMetadata(), command, ctx, stateTransferFlag, l1Invalidation);
      }
   }
}
