package org.infinispan.util;

import static org.infinispan.metadata.impl.PrivateMetadata.getBuilder;

import java.util.Optional;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracTombstoneManager;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.metadata.impl.PrivateMetadata;
import org.infinispan.util.logging.LogSupplier;

/**
 * Utility methods from IRAC (async cross-site replication)
 *
 * @author Pedro Ruivo
 * @since 12.0
 */
public final class IracUtils {

   private IracUtils() {
   }

   public static Optional<IracMetadata> findIracMetadataFromCacheEntry(CacheEntry<?, ?> entry) {
      PrivateMetadata privateMetadata = entry.getInternalMetadata();
      if (privateMetadata == null) {
         return Optional.empty();
      }
      return Optional.ofNullable(privateMetadata.iracMetadata());
   }

   public static IracEntryVersion getIracVersionFromCacheEntry(CacheEntry<?,?> entry) {
      return findIracMetadataFromCacheEntry(entry).map(IracMetadata::getVersion).orElse(null);
   }

   /**
    * Stores the {@link IracMetadata} into {@link CacheEntry}.
    * <p>
    * If the {@link CacheEntry} is a remove, then the tombstone is added via {@link
    * IracTombstoneManager#storeTombstone(int, Object, IracMetadata)}.
    *
    * @param entry            The {@link CacheEntry} to update.
    * @param metadata         The {@link IracMetadata} to store.
    * @param versionGenerator The {@link IracTombstoneManager} to update.
    * @param logSupplier      The {@link LogSupplier} to log the {@link IracMetadata} and the key.
    */
   public static void setIracMetadata(CacheEntry<?, ?> entry, int segment, IracMetadata metadata,
                                      IracTombstoneManager versionGenerator, LogSupplier logSupplier) {
      final Object key = entry.getKey();
      assert metadata != null : "[IRAC] Metadata must not be null!";
      if (entry.isRemoved()) {
         logTombstoneAssociated(key, metadata, logSupplier);
         versionGenerator.storeTombstone(segment, key, metadata);
      } else {
         logIracMetadataAssociated(key, metadata, logSupplier);
         updateCacheEntryMetadata(entry, metadata);
         versionGenerator.removeTombstone(key);
      }
   }

   /**
    * Same as {@link #setIracMetadata(CacheEntry, int, IracMetadata, IracTombstoneManager, LogSupplier)} but it stores a
    * "full" {@link PrivateMetadata} instead of {@link IracMetadata}.
    * <p>
    * This method is invoked to set the version from remote site updates. Note that the tombstone is not stored in case
    * of a remove operation.
    *
    * @param entry            The {@link CacheEntry} to update.
    * @param metadata         The {@link PrivateMetadata} to store.
    * @param versionGenerator The {@link IracTombstoneManager} to update.
    * @param logSupplier      The {@link LogSupplier} to log the {@link PrivateMetadata} and the key.
    */
   public static void setPrivateMetadata(CacheEntry<?, ?> entry, int segment, PrivateMetadata metadata,
                                         IracTombstoneManager versionGenerator, LogSupplier logSupplier) {
      final Object key = entry.getKey();
      assert metadata.iracMetadata() != null : "[IRAC] Metadata must not be null!";
      if (entry.isRemoved()) {
         logTombstoneAssociated(key, metadata.iracMetadata(), logSupplier);
         versionGenerator.storeTombstone(segment, key, metadata.iracMetadata());
      } else {
         logIracMetadataAssociated(key, metadata.iracMetadata(), logSupplier);
         entry.setInternalMetadata(metadata);
         versionGenerator.removeTombstone(key);
      }
   }

   public static void logUpdateDiscarded(Object key, IracMetadata metadata, LogSupplier logSupplier) {
      if (logSupplier.isTraceEnabled()) {
         logSupplier.getLog().tracef("[IRAC] Update from remote site discarded. Metadata=%s, key=%s", metadata, key);
      }
   }

   private static void logIracMetadataAssociated(Object key, IracMetadata metadata, LogSupplier logSupplier) {
      if (logSupplier.isTraceEnabled()) {
         logSupplier.getLog().tracef("[IRAC] IracMetadata %s associated with key '%s'", metadata, key);
      }
   }

   private static void logTombstoneAssociated(Object key, IracMetadata metadata, LogSupplier logSupplier) {
      if (logSupplier.isTraceEnabled()) {
         logSupplier.getLog().tracef("[IRAC] Store tombstone %s for key '%s'", metadata, key);
      }
   }

   private static void updateCacheEntryMetadata(CacheEntry<?, ?> entry, IracMetadata iracMetadata) {
      PrivateMetadata internalMetadata = getBuilder(entry.getInternalMetadata())
            .iracMetadata(iracMetadata)
            .build();
      entry.setInternalMetadata(internalMetadata);
   }

}
