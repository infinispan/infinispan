package org.infinispan.transaction.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.VersionedRepeatableReadEntry;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.persistence.util.EntryLoader;
import org.infinispan.remoting.responses.PrepareResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * Encapsulates write skew logic in maintaining version maps, etc.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class WriteSkewHelper {

   public static void mergePrepareResponses(Response r, PrepareResponse aggregateResponse) {
      if (r instanceof PrepareResponse && aggregateResponse != null) {
         PrepareResponse remoteRsp = (PrepareResponse) r;
         aggregateResponse.merge(remoteRsp);
      }
   }

   public static PrepareResponse mergeInPrepareResponse(Map<Object, IncrementableEntryVersion> versionsMap, PrepareResponse response) {
      response.mergeEntryVersions(versionsMap);
      return response;
   }

   public static Map<Object, IncrementableEntryVersion> mergeEntryVersions(Map<Object, IncrementableEntryVersion> entryVersions,
                                                                           Map<Object, IncrementableEntryVersion> updatedEntryVersions) {
      if (updatedEntryVersions != null && !updatedEntryVersions.isEmpty()){
         updatedEntryVersions.putAll(entryVersions);
         return updatedEntryVersions;
      }
      return entryVersions;
   }

   public static CompletionStage<Map<Object, IncrementableEntryVersion>> performWriteSkewCheckAndReturnNewVersions(VersionedPrepareCommand prepareCommand,
                                                                                                                   EntryLoader<?, ?> entryLoader,
                                                                                                                   VersionGenerator versionGenerator,
                                                                                                                   TxInvocationContext<?> context,
                                                                                                                   KeySpecificLogic ksl,
                                                                                                                   KeyPartitioner keyPartitioner) {
      if (prepareCommand.getVersionsSeen() == null) {
         // Do not perform the write skew check if this prepare command is being replayed for state transfer
         return CompletableFutures.completedEmptyMap();
      }

      Map<Object, IncrementableEntryVersion> uv = new HashMap<>();
      AggregateCompletionStage<Map<Object, IncrementableEntryVersion>> aggregateCompletionStage = CompletionStages.aggregateCompletionStage(uv);

      for (WriteCommand c : prepareCommand.getModifications()) {
         for (Object k : c.getAffectedKeys()) {
            int segment = SegmentSpecificCommand.extractSegment(c, k, keyPartitioner);
            if (ksl.performCheckOnSegment(segment)) {
               CacheEntry<?, ?> cacheEntry = context.lookupEntry(k);
               if (!(cacheEntry instanceof VersionedRepeatableReadEntry)) {
                  continue;
               }
               VersionedRepeatableReadEntry entry = (VersionedRepeatableReadEntry) cacheEntry;

               CompletionStage<Boolean> skewStage = entry.performWriteSkewCheck(entryLoader, segment, context,
                     prepareCommand.getVersionsSeen().get(k), versionGenerator, c.hasAnyFlag(FlagBitSets.ROLLING_UPGRADE));
               aggregateCompletionStage.dependsOn(skewStage.thenAccept(passSkew -> {
                  if (!passSkew) {
                     throw new WriteSkewException("Write skew detected on key " + entry.getKey() + " for transaction " +
                           context.getCacheTransaction(), entry.getKey());
                  }
                  IncrementableEntryVersion newVersion = incrementVersion(entry,versionGenerator);
                  // Have to synchronize as we could have returns on different threads due to notifications/loaders etc
                  synchronized (uv) {
                     uv.put(entry.getKey(), newVersion);
                  }
               }));
            }
         }
      }
      return aggregateCompletionStage.freeze();
   }

   public static IncrementableEntryVersion versionFromEntry(CacheEntry<?, ?> entry) {
      if (entry == null) {
         return null;
      }
      PrivateMetadata metadata = entry.getInternalMetadata();
      return metadata == null ? null : metadata.entryVersion();
   }

   public static IncrementableEntryVersion incrementVersion(CacheEntry<?, ?> entry, VersionGenerator versionGenerator) {
      IncrementableEntryVersion oldVersion = versionFromEntry(entry);
      return entry.isCreated() || oldVersion == null ?
            versionGenerator.generateNew() :
            versionGenerator.increment(oldVersion);
   }

   public interface KeySpecificLogic {
      boolean performCheckOnSegment(int segment);
   }

   public static final KeySpecificLogic ALWAYS_TRUE_LOGIC = k -> true;
}
