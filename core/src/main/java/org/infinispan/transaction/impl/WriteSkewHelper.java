package org.infinispan.transaction.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.commands.SegmentSpecificCommand;
import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.VersionedRepeatableReadEntry;
import org.infinispan.container.versioning.EntryVersionsMap;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.persistence.util.EntryLoader;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.transaction.WriteSkewException;
import org.infinispan.transaction.xa.CacheTransaction;
import org.infinispan.util.concurrent.AggregateCompletionStage;
import org.infinispan.util.concurrent.CompletionStages;

/**
 * Encapsulates write skew logic in maintaining version maps, etc.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class WriteSkewHelper {

   public static void readVersionsFromResponse(Response r, CacheTransaction ct) {
      if (r != null && r.isSuccessful()) {
         SuccessfulResponse sr = (SuccessfulResponse) r;
         EntryVersionsMap uv = (EntryVersionsMap) sr.getResponseValue();
         if (uv != null) ct.setUpdatedEntryVersions(uv.merge(ct.getUpdatedEntryVersions()));
      }
   }

   public static CompletionStage<EntryVersionsMap> performWriteSkewCheckAndReturnNewVersions(VersionedPrepareCommand prepareCommand,
                                                                                            EntryLoader entryLoader,
                                                                                            VersionGenerator versionGenerator,
                                                                                            TxInvocationContext context,
                                                                                            KeySpecificLogic ksl,
                                                                                            KeyPartitioner keyPartitioner) {
      EntryVersionsMap uv = new EntryVersionsMap();
      if (prepareCommand.getVersionsSeen() == null) {
         // Do not perform the write skew check if this prepare command is being replayed for state transfer
         return CompletableFuture.completedFuture(uv);
      }

      AggregateCompletionStage<EntryVersionsMap> aggregateCompletionStage = CompletionStages.aggregateCompletionStage(uv);

      for (WriteCommand c : prepareCommand.getModifications()) {
         for (Object k : c.getAffectedKeys()) {
            int segment = SegmentSpecificCommand.extractSegment(c, k, keyPartitioner);
            if (ksl.performCheckOnSegment(segment)) {
               CacheEntry cacheEntry = context.lookupEntry(k);
               if (!(cacheEntry instanceof VersionedRepeatableReadEntry)) {
                  continue;
               }
               VersionedRepeatableReadEntry entry = (VersionedRepeatableReadEntry) cacheEntry;

               CompletionStage<Boolean> skewStage = entry.performWriteSkewCheck(entryLoader, segment, context,
                     prepareCommand.getVersionsSeen().get(k), versionGenerator);
               aggregateCompletionStage.dependsOn(skewStage.thenAccept(passSkew -> {
                  if (passSkew) {
                     IncrementableEntryVersion oldVersion = (IncrementableEntryVersion) entry.getMetadata().version();
                     IncrementableEntryVersion newVersion = entry.isCreated() || oldVersion == null
                           ? versionGenerator.generateNew()
                           : versionGenerator.increment(oldVersion);
                     // Have to synchronize as we could have returns on different threads due to notifications/loaders etc
                     synchronized (uv) {
                        uv.put(entry.getKey(), newVersion);
                     }
                  } else {
                     // Write skew check detected!
                     throw new WriteSkewException("Write skew detected on key " + k + " for transaction " +
                           context.getCacheTransaction(), k);
                  }
               }));
            }
         }
      }
      return aggregateCompletionStage.freeze();
   }

   public static CompletionStage<EntryVersionsMap> performTotalOrderWriteSkewCheckAndReturnNewVersions(VersionedPrepareCommand prepareCommand,
                                                                                      EntryLoader entryLoader,
                                                                                      VersionGenerator versionGenerator,
                                                                                      TxInvocationContext context,
                                                                                      KeySpecificLogic ksl,
                                                                                      KeyPartitioner keyPartitioner) {
      EntryVersionsMap uv = new EntryVersionsMap();
      AggregateCompletionStage<EntryVersionsMap> aggregateCompletionStage = CompletionStages.aggregateCompletionStage(uv);
      for (WriteCommand c : prepareCommand.getModifications()) {
         for (Object k : c.getAffectedKeys()) {
            int segment = SegmentSpecificCommand.extractSegment(c, k, keyPartitioner);
            if (ksl.performCheckOnSegment(segment)) {
               VersionedRepeatableReadEntry entry = (VersionedRepeatableReadEntry) context.lookupEntry(k);

               CompletionStage<Boolean> skewStage = entry.performWriteSkewCheck(entryLoader, segment, context,
                     prepareCommand.getVersionsSeen().get(k), versionGenerator);
               aggregateCompletionStage.dependsOn(skewStage.thenAccept(passSkew -> {
                  if (passSkew) {
                     //in total order, it does not care about the version returned. It just need the keys validated
                     synchronized (uv) {
                        uv.put(k, null);
                     }
                  } else {
                     // Write skew check detected!
                     throw new WriteSkewException("Write skew detected on key " + k + " for transaction " +
                           context.getCacheTransaction(), k);
                  }
               }));
            }
         }
      }
      return aggregateCompletionStage.freeze();
   }

   public interface KeySpecificLogic {
      boolean performCheckOnSegment(int segment);
   }

   public static final KeySpecificLogic ALWAYS_TRUE_LOGIC = k -> true;
}
