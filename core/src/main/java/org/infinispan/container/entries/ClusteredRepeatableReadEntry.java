package org.infinispan.container.entries;

import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.metadata.Metadata;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.versioned.Versioned;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.container.versioning.InequalVersionComparisonResult;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.persistence.PersistenceUtil;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.util.TimeService;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicReference;

import static org.infinispan.commons.util.Util.toStr;

/**
 * A version of RepeatableReadEntry that can perform write-skew checks during prepare.
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class ClusteredRepeatableReadEntry extends RepeatableReadEntry implements Versioned {

   private static final Log log = LogFactory.getLog(ClusteredRepeatableReadEntry.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final AtomicReference<Boolean> ignored = new AtomicReference<>();

   public ClusteredRepeatableReadEntry(Object key, Object value, Metadata metadata) {
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
      InternalCacheEntry ice = PersistenceUtil.loadAndStoreInDataContainer(container, persistenceManager, getKey(),
                                                                           ctx, timeService, ignored);
      if (ice == null) {
         if (trace) {
            log.tracef("No entry for key %s found in data container" , toStr(key));
         }
         prevVersion = ctx.getCacheTransaction().getLookedUpRemoteVersion(key);
         if (prevVersion == null) {
            if (trace) {
               log.tracef("No looked up remote version for key %s found in context" , toStr(key));
            }
            //in this case, the key does not exist. So, the only result possible is the version seen be the NonExistingVersion
            return versionGenerator.nonExistingVersion().compareTo(versionSeen) == InequalVersionComparisonResult.EQUAL;
         }
      } else {
         prevVersion = ice.getMetadata().version();
         if (prevVersion == null)
            throw new IllegalStateException("Entries cannot have null versions!");
      }
      if (trace) {
         log.tracef("Is going to compare versions %s and %s for key %s.", prevVersion, versionSeen, toStr(key));
      }

      //in this case, the transaction read some value and the data container has a value stored.
      //version seen and previous version are not null. Simple version comparation.
      InequalVersionComparisonResult result = prevVersion.compareTo(versionSeen);
      if (trace) {
         log.tracef("Comparing versions %s and %s for key %s: %s", prevVersion, versionSeen, key, result);
      }
      return InequalVersionComparisonResult.AFTER != result;
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
   public boolean isNull() {
      return value == null;
   }

   @Override
   public ClusteredRepeatableReadEntry clone() {
      return (ClusteredRepeatableReadEntry) super.clone();
   }
}
