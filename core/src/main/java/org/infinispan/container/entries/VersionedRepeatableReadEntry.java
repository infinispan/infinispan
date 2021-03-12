package org.infinispan.container.entries;

import static org.infinispan.commons.util.Util.toStr;
import static org.infinispan.transaction.impl.WriteSkewHelper.versionFromEntry;

import java.util.concurrent.CompletionStage;

import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.IncrementableEntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.util.EntryLoader;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A version of RepeatableReadEntry that can perform write-skew checks during prepare.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedRepeatableReadEntry<K, V> extends RepeatableReadEntry<K, V> {

   private static final Log log = LogFactory.getLog(VersionedRepeatableReadEntry.class);

   public VersionedRepeatableReadEntry(K key, V value, Metadata metadata) {
      super(key, value, metadata);
   }

   /**
    *
    * @param segment the segment matching this entry
    * @param ctx the invocation context
    * @param versionSeen what version has been seen for this entry
    * @param versionGenerator generator to generate a new version if needed
    * @param rollingUpgrade
    * @return whether a write skew occurred for this entry
    */
   public CompletionStage<Boolean> performWriteSkewCheck(EntryLoader<K, V> entryLoader, int segment,
                                        TxInvocationContext<?> ctx, EntryVersion versionSeen,
                                        VersionGenerator versionGenerator, boolean rollingUpgrade) {
      if (versionSeen == null) {
         if (log.isTraceEnabled()) {
            log.tracef("Perform write skew check for key %s but the key was not read. Skipping check!", toStr(key));
         }
         //version seen is null when the entry was not read. In this case, the write skew is not needed.
         return CompletableFutures.completedTrue();
      }
      CompletionStage<IncrementableEntryVersion> entryStage;
      if (ctx.isOriginLocal()) {
         entryStage = getCurrentEntryVersion(entryLoader, segment, ctx, versionGenerator, rollingUpgrade);
      } else {
         // If this node is an owner and not originator, the entry has been loaded and wrapped under lock,
         // so the version in context should be up-to-date
         IncrementableEntryVersion prevVersion = ctx.getCacheTransaction().getVersionsRead().get(key);
         if (prevVersion == null) {
            // If the command has IGNORE_RETURN_VALUE flags it's possible that the entry was not loaded
            // from cache loader - we have to force load
            entryStage = getCurrentEntryVersion(entryLoader, segment, ctx, versionGenerator, rollingUpgrade);
         } else {
            return CompletableFutures.booleanStage(skewed(prevVersion, versionSeen, versionGenerator));
         }
      }
      return entryStage.thenApply(prevVersion -> skewed(prevVersion, versionSeen, versionGenerator));
   }

   private boolean skewed(IncrementableEntryVersion prevVersion, EntryVersion versionSeen, VersionGenerator versionGenerator) {
      // If it is expired then it is possible the previous version doesn't exist - because entry didn't exist)
      if (isExpired() && prevVersion == versionGenerator.nonExistingVersion()) {
         return true;
      }

      //in this case, the transaction read some value and the data container has a value stored.
      //version seen and previous version are not null. Simple version comparation.
      InequalVersionComparisonResult result = prevVersion.compareTo(versionSeen);
      if (log.isTraceEnabled()) {
         log.tracef("Comparing versions %s and %s for key %s: %s", prevVersion, versionSeen, key, result);
      }
      // TODO: there is a risk associated with versions that are not monotonous per entry - if an entry is removed
      // and then written several times, it can reach the previous version.
      return InequalVersionComparisonResult.EQUAL == result;
   }

   private CompletionStage<IncrementableEntryVersion> getCurrentEntryVersion(EntryLoader<K, V> entryLoader, int segment, TxInvocationContext ctx, VersionGenerator versionGenerator, boolean rollingUpgrade) {
      // TODO: persistence should be more orthogonal to any entry type - this should be handled in interceptor
      // on origin, the version seen is acquired without the lock, so we have to retrieve it again
      CompletionStage<InternalCacheEntry<K, V>> entry = entryLoader.loadAndStoreInDataContainer(ctx, getKey(), segment, null);

      return entry.thenApply(ice -> {
         if (ice == null) {
            if (log.isTraceEnabled()) {
               log.tracef("No entry for key %s found in data container", toStr(key));
            }
            //in this case, the key does not exist. So, the only result possible is the version seen be the NonExistingVersion
            return versionGenerator.nonExistingVersion();
         }

         if (log.isTraceEnabled()) {
            log.tracef("Entry found in data container: %s", toStr(ice));
         }
         IncrementableEntryVersion prevVersion = versionFromEntry(ice);
         if (prevVersion == null) {
            if (rollingUpgrade) {
               return versionGenerator.nonExistingVersion();
            }
            throw new IllegalStateException("Entries cannot have null versions!");
         }
         return prevVersion;
      });
   }

   @Override
   public VersionedRepeatableReadEntry<K, V> clone() {
      return (VersionedRepeatableReadEntry<K, V>) super.clone();
   }
}
