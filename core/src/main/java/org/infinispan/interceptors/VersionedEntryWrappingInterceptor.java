package org.infinispan.interceptors;

import org.infinispan.context.Flag;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Interceptor in charge with wrapping entries and add them in caller's context.
 *
 * @author Mircea Markus
 * @since 5.1
 */
public class VersionedEntryWrappingInterceptor extends EntryWrappingInterceptor {

   protected VersionGenerator versionGenerator;
   private static final Log log = LogFactory.getLog(VersionedEntryWrappingInterceptor.class);

   @Override
   protected Log getLog() {
      return log;
   }

   @Inject
   public void initialize(VersionGenerator versionGenerator) {
      this.versionGenerator = versionGenerator;
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         ((VersionedPrepareCommand) command).setVersionsSeen(ctx.getCacheTransaction().getVersionsRead());
      }
      wrapEntriesForPrepare(ctx, command);
      EntryVersionsMap newVersionData= null;
      if (ctx.isOriginLocal() && !ctx.getCacheTransaction().isFromStateTransfer()) newVersionData = cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx, (VersionedPrepareCommand) command);

      Object retval = invokeNextInterceptor(ctx, command);

      if (!ctx.isOriginLocal()) newVersionData = cdl.createNewVersionsAndCheckForWriteSkews(versionGenerator, ctx, (VersionedPrepareCommand) command);
      if (command.isOnePhaseCommit()) ctx.getCacheTransaction().setUpdatedEntryVersions(((VersionedPrepareCommand) command).getVersionsSeen());

      if (newVersionData != null) retval = newVersionData;
      if (command.isOnePhaseCommit()) commitContextEntries(ctx, null, null);
      return retval;
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      try {
         if (ctx.isOriginLocal())
            ((VersionedCommitCommand) command).setUpdatedVersions(ctx.getCacheTransaction().getUpdatedEntryVersions());

         return invokeNextInterceptor(ctx, command);
      } finally {
         if (!ctx.isOriginLocal())
            ctx.getCacheTransaction().setUpdatedEntryVersions(((VersionedCommitCommand) command).getUpdatedVersions());
         commitContextEntries(ctx, null, null);
      }
   }

   @Override
   protected void commitContextEntry(CacheEntry entry, InvocationContext ctx, FlagAffectedCommand command,
                                     Metadata metadata, Flag stateTransferFlag, boolean l1Invalidation) {
      if (ctx.isInTxScope() && stateTransferFlag == null) {
         EntryVersion updatedEntryVersion = ((TxInvocationContext) ctx)
               .getCacheTransaction().getUpdatedEntryVersions().get(entry.getKey());
         Metadata commitMetadata;
         if (updatedEntryVersion != null) {
            if (metadata == null && entry.getMetadata() == null)
               commitMetadata = new EmbeddedMetadata.Builder().version(updatedEntryVersion).build();
            else if (metadata != null)
               commitMetadata = metadata.builder().version(updatedEntryVersion).build();
            else
               commitMetadata = entry.getMetadata().builder().version(updatedEntryVersion).build();
         } else {
            commitMetadata = metadata != null ? metadata : entry.getMetadata();
         }

         cdl.commitEntry(entry, commitMetadata, command, ctx, null, l1Invalidation);
      } else {
         // This could be a state transfer call!
         cdl.commitEntry(entry, entry.getMetadata(), command, ctx, stateTransferFlag, l1Invalidation);
      }
   }

}
