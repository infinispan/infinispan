package org.infinispan.interceptors.xsite;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.functional.ReadWriteKeyCommand;
import org.infinispan.commands.functional.ReadWriteKeyValueCommand;
import org.infinispan.commands.functional.ReadWriteManyCommand;
import org.infinispan.commands.functional.ReadWriteManyEntriesCommand;
import org.infinispan.commands.functional.WriteOnlyKeyCommand;
import org.infinispan.commands.functional.WriteOnlyKeyValueCommand;
import org.infinispan.commands.functional.WriteOnlyManyCommand;
import org.infinispan.commands.functional.WriteOnlyManyEntriesCommand;
import org.infinispan.commands.write.ComputeCommand;
import org.infinispan.commands.write.ComputeIfAbsentCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.impl.InternalEntryFactory;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.encoding.DataConversion;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.functional.impl.Params;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.InvocationSuccessFunction;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.marshall.core.MarshallableFunctions;

/**
 * Handles x-site data backups for non-transactional caches.
 *
 * @author Mircea Markus
 * @author Pedro Ruivo
 * @since 5.2
 */
public class NonTransactionalBackupInterceptor extends BaseBackupInterceptor {

   private final InvocationSuccessFunction<DataWriteCommand> handleSingleKeyWriteReturn = this::handleSingleKeyWriteReturn;
   private final InvocationSuccessFunction<WriteCommand> handleMultipleKeysWriteReturn = this::handleMultipleKeysWriteReturn;

   @Inject CommandsFactory commandsFactory;
   @Inject ClusteringDependentLogic clusteringDependentLogic;
   @Inject InternalEntryFactory internalEntryFactory;

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeCommand(InvocationContext ctx, ComputeCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitComputeIfAbsentCommand(InvocationContext ctx, ComputeIfAbsentCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyCommand(InvocationContext ctx, WriteOnlyKeyCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyValueCommand(InvocationContext ctx, ReadWriteKeyValueCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteKeyCommand(InvocationContext ctx, ReadWriteKeyCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyEntriesCommand(InvocationContext ctx, WriteOnlyManyEntriesCommand command) throws Throwable {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyKeyValueCommand(InvocationContext ctx, WriteOnlyKeyValueCommand command) throws Throwable {
      return handleSingleKeyWriteCommand(ctx, command);
   }

   @Override
   public Object visitWriteOnlyManyCommand(InvocationContext ctx, WriteOnlyManyCommand command) {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyCommand(InvocationContext ctx, ReadWriteManyCommand command) throws Throwable {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

   @Override
   public Object visitReadWriteManyEntriesCommand(InvocationContext ctx, ReadWriteManyEntriesCommand command) throws Throwable {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      return handleMultipleKeysWriteCommand(ctx, command);
   }

   private Object handleSingleKeyWriteCommand(InvocationContext ctx, DataWriteCommand command) {
      if (skipXSiteBackup(command)) {
         return invokeNext(ctx, command);
      }
      return invokeNextThenApply(ctx, command, handleSingleKeyWriteReturn);
   }

   private Object handleSingleKeyWriteReturn(InvocationContext ctx, DataWriteCommand dataWriteCommand, Object rv) throws Throwable {
      int segment = dataWriteCommand.getSegment();
      if (dataWriteCommand.isSuccessful() &&
            clusteringDependentLogic.getCacheTopology().getSegmentDistribution(segment).isPrimary()) {
         CacheEntry entry = ctx.lookupEntry(dataWriteCommand.getKey());
         DataWriteCommand crossSiteCommand;
         if (entry.isRemoved()) {
            crossSiteCommand = commandsFactory.buildRemoveCommand(dataWriteCommand.getKey(), null, segment,
                  dataWriteCommand.getFlagsBitSet());
         } else {
            crossSiteCommand = commandsFactory.buildPutKeyValueCommand(dataWriteCommand.getKey(), entry.getValue(),
                  segment, entry.getMetadata(), dataWriteCommand.getFlagsBitSet());
         }
         InvocationStage stage = backupSender.backupWrite(crossSiteCommand, dataWriteCommand);
         return stage.thenReturn(ctx, dataWriteCommand, rv);
      }
      return rv;
   }

   private Object handleMultipleKeysWriteCommand(InvocationContext ctx, WriteCommand command) {
      if (trace) log.tracef("Processing %s", command);
      if (skipXSiteBackup(command)) {
         return invokeNext(ctx, command);
      } else if (command instanceof ReadWriteKeyCommand) {
         ReadWriteKeyCommand readWriteKeyCommand = (ReadWriteKeyCommand) command;
         return commandsFactory.buildReadWriteKeyCommand(readWriteKeyCommand.getKey(), readWriteKeyCommand.getFunction(),
               readWriteKeyCommand.getSegment(), readWriteKeyCommand.getParams(), readWriteKeyCommand.getKeyDataConversion(), readWriteKeyCommand.getValueDataConversion());
      }
      return invokeNextThenApply(ctx, command, handleMultipleKeysWriteReturn);
   }

   private Object handleMultipleKeysWriteReturn(InvocationContext ctx, WriteCommand writeCommand, Object rv) throws Throwable {
      if (trace) log.tracef("Processing post %s", writeCommand);
      if (!writeCommand.isSuccessful()) {
         if (trace) {
            log.tracef("Command %s is not succesful, not replicating", writeCommand);
         }
         return rv;
      }
      Map<Object, Object> map = new HashMap<>(); // must support null values
      LocalizedCacheTopology localizedCacheTopology = clusteringDependentLogic.getCacheTopology();
      for (Object key : writeCommand.getAffectedKeys()) {
         DistributionInfo info = localizedCacheTopology.getDistribution(key);
         if (!info.isPrimary()) {
            if (trace) {
               log.tracef("Not replicating write to key %s as the primary owner is %s", key, info.primary());
            }
            continue;
         }
         CacheEntry entry = ctx.lookupEntry(key);
         if (entry instanceof InternalCacheEntry) {
            map.put(key, ((InternalCacheEntry) entry).toInternalCacheValue());
         } else {
            map.put(key, internalEntryFactory.createValue(entry));
         }
      }
      if (map.isEmpty()) {
         return rv;
      }
      //TODO: Converters
      WriteOnlyManyEntriesCommand crossSiteCommand = commandsFactory.buildWriteOnlyManyEntriesCommand(map,
            MarshallableFunctions.setInternalCacheValueConsumer(), Params.fromFlagsBitSet(writeCommand.getFlagsBitSet()), DataConversion.DEFAULT_KEY, DataConversion.DEFAULT_VALUE);
      InvocationStage stage = backupSender.backupWrite(crossSiteCommand, writeCommand);
      return stage.thenReturn(ctx, writeCommand, rv);
   }
}
