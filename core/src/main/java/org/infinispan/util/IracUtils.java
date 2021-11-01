package org.infinispan.util;

import static org.infinispan.metadata.impl.PrivateMetadata.getBuilder;

import java.util.Optional;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
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
    * IracVersionGenerator#storeTombstone(Object, IracMetadata)}.
    *
    * @param entry            The {@link CacheEntry} to update.
    * @param metadata         The {@link IracMetadata} to store.
    * @param versionGenerator The {@link IracVersionGenerator} to update.
    * @param logSupplier      The {@link LogSupplier} to log the {@link IracMetadata} and the key.
    */
   public static void setIracMetadata(CacheEntry<?, ?> entry, IracMetadata metadata,
         IracVersionGenerator versionGenerator, LogSupplier logSupplier) {
      final Object key = entry.getKey();
      assert metadata != null : "[IRAC] Metadata must not be null!";
      if (entry.isRemoved()) {
         logTombstoneAssociated(key, metadata, logSupplier);
         versionGenerator.storeTombstone(key, metadata);
      } else {
         logIracMetadataAssociated(key, metadata, logSupplier);
         updateCacheEntryMetadata(entry, metadata);
         versionGenerator.removeTombstone(key);
      }
   }

   /**
    * Same as {@link #setIracMetadata(CacheEntry, IracMetadata, IracVersionGenerator, LogSupplier)} but it stores a
    * "full" {@link PrivateMetadata} instead of {@link IracMetadata}.
    *
    * @param entry            The {@link CacheEntry} to update.
    * @param metadata         The {@link PrivateMetadata} to store.
    * @param versionGenerator The {@link IracVersionGenerator} to update.
    * @param logSupplier      The {@link LogSupplier} to log the {@link PrivateMetadata} and the key.
    */
   public static void setPrivateMetadata(CacheEntry<?, ?> entry, PrivateMetadata metadata,
         IracVersionGenerator versionGenerator, LogSupplier logSupplier) {
      final Object key = entry.getKey();
      assert metadata.iracMetadata() != null : "[IRAC] Metadata must not be null!";
      // TODO: If the entry is removed don't register the tombstone
      if (!entry.isRemoved()) {
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
