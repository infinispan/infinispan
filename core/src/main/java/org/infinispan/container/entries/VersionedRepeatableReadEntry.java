package org.infinispan.container.entries;

import static org.infinispan.commons.util.Util.toStr;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.versioned.Versioned;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.metadata.Metadata;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A version of RepeatableReadEntry that can perform write-skew checks during prepare.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class VersionedRepeatableReadEntry extends RepeatableReadEntry implements Versioned {

   private static final Log log = LogFactory.getLog(VersionedRepeatableReadEntry.class);
   private static final boolean trace = log.isTraceEnabled();

   public VersionedRepeatableReadEntry(Object key, Object value, Metadata metadata) {
      super(key, value, metadata);
   }

   public boolean performWriteSkewCheck(DataContainer container, PersistenceManager persistenceManager,
                                        TxInvocationContext ctx, EntryVersion versionSeen,
                                        VersionGenerator versionGenerator, TimeService timeService) {
      if (versionSeen == null) {
         if (trace) {
            log.tracef("Perform write skew check for key %s but the key was not read. Skipping check!", toStr(key));
         }
         //version seen is null when the entry was not read. In this case, the write skew is not needed.
         return true;
      }
      EntryVersion prevVersion;
      if (ctx.isOriginLocal()) {
         prevVersion = getCurrentEntryVersion(container, persistenceManager, ctx, versionGenerator, timeService);
      } else {
         // If this node is an owner and not originator, the entry has been loaded and wrapped under lock,
         // so the version in context should be up-to-date
         prevVersion = ctx.getCacheTransaction().getVersionsRead().get(key);
         if (prevVersion == null && !isLoaded()) {
            prevVersion = getCurrentEntryVersion(container, persistenceManager, ctx, versionGenerator, timeService);
         }
      }

      //in this case, the transaction read some value and the data container has a value stored.
      //version seen and previous version are not null. Simple version comparation.
      InequalVersionComparisonResult result = prevVersion.compareTo(versionSeen);
      if (trace) {
         log.tracef("Comparing versions %s and %s for key %s: %s", prevVersion, versionSeen, key, result);
      }
      // TODO: there is a risk associated with versions that are not monotonous per entry - if an entry is removed
      // and then written several times, it can reach the previous version.
      return InequalVersionComparisonResult.EQUAL == result;
   }

   private EntryVersion getCurrentEntryVersion(DataContainer container, PersistenceManager persistenceManager, TxInvocationContext ctx, VersionGenerator versionGenerator, TimeService timeService) {
      EntryVersion prevVersion;// on origin, the version seen is acquired without the lock, so we have to retrieve it again
      // TODO: persistence should be more orthogonal to any entry type - this should be handled in interceptor
      InternalCacheEntry ice = PersistenceUtil.loadAndStoreInDataContainer(container, persistenceManager, getKey(),
            ctx, timeService, null);
      if (ice == null || ice.getValue() == null) {
         if (trace) {
            log.tracef("No entry for key %s found in data container", toStr(key));
         }
         //in this case, the key does not exist. So, the only result possible is the version seen be the NonExistingVersion
         prevVersion = versionGenerator.nonExistingVersion();
      } else {
         prevVersion = ice.getMetadata().version();
         if (prevVersion == null) {
            // This should happen only when the version was not inserted into store for some reason (as in test)
            prevVersion = versionGenerator.nonExistingVersion();
         }
      }
      return prevVersion;
   }

   // This entry is only used when versioning is enabled, and in these
   // situations, versions are generated internally and assigned at a
   // different stage to the rest of metadata. So, keep the versioned API
   // to make it easy to apply version information when needed.

   @Override
   public EntryVersion getVersion() {
      return metadata.version();
   }

   @Override
   public void setVersion(EntryVersion version) {
      metadata = metadata.builder().version(version).build();
   }

   @Override
   public VersionedRepeatableReadEntry clone() {
      return (VersionedRepeatableReadEntry) super.clone();
   }
}
