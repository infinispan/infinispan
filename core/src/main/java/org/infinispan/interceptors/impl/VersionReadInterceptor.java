package org.infinispan.interceptors.impl;

import java.util.Collection;
import java.util.Map;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.functional.ReadOnlyKeyCommand;
import org.infinispan.commands.functional.ReadOnlyManyCommand;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.read.GetAllCommand;
import org.infinispan.commands.read.GetCacheEntryCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.remote.GetKeysInGroupCommand;
import org.infinispan.commands.write.ApplyDeltaCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.EvictCommand;
import org.infinispan.commands.write.InvalidateCommand;
import org.infinispan.commands.write.InvalidateL1Command;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.MVCCEntry;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Records versions read just before executing the command which could modify the metadata.
 */
public class VersionReadInterceptor extends DDAsyncInterceptor {
   private static final Log log = LogFactory.getLog(VersionReadInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private VersionGenerator versionGenerator;

   @Inject
   public void init(VersionGenerator versionGenerator) {
      this.versionGenerator = versionGenerator;
   }

   private Object handleDataCommand(InvocationContext ctx, DataCommand command) {
      if (ctx.isInTxScope()) {
         CacheEntry cacheEntry = ctx.lookupEntry(command.getKey());
         addVersionRead((TxInvocationContext) ctx, (MVCCEntry) cacheEntry);
      }
      return invokeNext(ctx, command);
   }

   private Object handleManyCommand(InvocationContext ctx, VisitableCommand command, Collection<?> keys) {
      if (ctx.isInTxScope()) {
         TxInvocationContext txCtx = (TxInvocationContext) ctx;
         for (Object key : keys) {
            addVersionRead(txCtx, (MVCCEntry) ctx.lookupEntry(key));
         }
      }
      return invokeNext(ctx, command);
   }

   private void addVersionRead(TxInvocationContext ctx, MVCCEntry entry) {
      if (entry.getMetadata() == null) {
         entry.setMetadata(new EmbeddedMetadata.Builder().version(versionGenerator.nonExistingVersion()).build());
      }
      if (!ctx.isOriginLocal()) {
         // If this is origin, we'll add the version to seen versions anyway. When this node will be committing
         // the record it has to reload due to WSC as the load here does not happen with lock.
         if (!entry.isLoaded()) {
            if (trace) log.trace("Version was not loaded from persistence, we need to reload during write skew check.");
            return;
         }
         // On non-origin we'll add the version even if it was not read, for write-skew check as it was already loaded
         // for listeners.
      } else if (!entry.isRead()) {
         if (trace) log.trace("Entry was not read, not adding to context");
         return;
      }
      EntryVersion version = entry.getMetadata().version();
      if (entry.getValue() == null || version == null) {
         if (trace) log.tracef("Adding non-existent version read for key %s", entry.getKey());
         version = versionGenerator.nonExistingVersion();
      } else {
         if (trace) log.tracef("Adding version read %s for key %s", version, entry.getKey());
      }
      ctx.getCacheTransaction().addVersionRead(entry.getKey(), version);
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleManyCommand(ctx, command, command.getAffectedKeys());
   }

   @Override
   public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitApplyDeltaCommand(InvocationContext ctx, ApplyDeltaCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitGetCacheEntryCommand(InvocationContext ctx, GetCacheEntryCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitGetAllCommand(InvocationContext ctx, GetAllCommand command) throws Throwable {
      return handleManyCommand(ctx, command, command.getKeys());
   }

   @Override
   public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand command) throws Throwable {
      return handleManyCommand(ctx, command, command.getAffectedKeys());
   }

   @Override
   public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command command) throws Throwable {
      return handleManyCommand(ctx, command, command.getAffectedKeys());
   }

   @Override
   public Object visitGetKeysInGroupCommand(InvocationContext ctx, GetKeysInGroupCommand command) throws Throwable {
      if (ctx.isOriginLocal() && ctx.isInTxScope()) {
         TxInvocationContext txCtx = (TxInvocationContext) ctx;
         for (Map.Entry<Object, CacheEntry> keyEntry : txCtx.getLookedUpEntries().entrySet()) {
            addVersionRead(txCtx, (MVCCEntry) keyEntry.getValue());
         }
      }
      return invokeNext(ctx, command);
   }

   @Override
   public Object visitReadOnlyKeyCommand(InvocationContext ctx, ReadOnlyKeyCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitReadOnlyManyCommand(InvocationContext ctx, ReadOnlyManyCommand command) throws Throwable {
      return handleManyCommand(ctx, command, command.getKeys());
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleManyCommand(ctx, command, command.getAffectedKeys());
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) throws Throwable {
      return handleManyCommand(ctx, command, command.getAffectedKeys());
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleManyCommand(ctx, command, command.getAffectedKeys());
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return handleManyCommand(ctx, command, command.getAffectedKeys());
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }
}
