package org.infinispan.container.versioning.irac;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.topology.CacheTopology;

/**
 * A version generator for the IRAC protocol.
 * <p>
 * It also stores the tombstone from the keys removed.
 * <p>
 * The version is segment based and the new version is also after than the previous one.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public interface IracVersionGenerator extends Lifecycle {

   /**
    * Generates a new {@link IracMetadata} for a given {@code segment}.
    * <p>
    * The {@link IracEntryVersion} created is always higher than the previous one for the same {@code segment}.
    *
    * @param segment The segment.
    * @return The {@link IracMetadata} created.
    */
   IracMetadata generateNewMetadata(int segment);

   /**
    * Generate a new {@link IracMetadata} for a given {@code segment}.
    * <p>
    * The {@link IracEntryVersion} created will be the same as the previous one for the same {@code segment}. If there
    * was no version prior then it will create an initial version.
    *
    * @param segment The segment.
    * @return The {@link IracMetadata} created.
    */
   IracMetadata generateMetadataWithCurrentVersion(int segment);

   /**
    * Creates a new {@link IracMetadata} with the merged {@link IracEntryVersion} from locally stored value and remote
    * value.
    * <p>
    * It also updates the version to ensure {@link #generateNewMetadata(int)} generates a new version higher than both.
    *
    * @param segment       The segment.
    * @param localVersion  The {@link IracEntryVersion} stored locally.
    * @param remoteVersion The {@link IracEntryVersion} received from the remote site.
    * @param siteName      The site name to store in {@link IracMetadata}.
    * @return The {@link IracMetadata} instance.
    */
   IracMetadata mergeVersion(int segment, IracEntryVersion localVersion, IracEntryVersion remoteVersion,
         String siteName);

   /**
    * Updates the version for the {@code segment} with a new {@code remoteVersion} seen.
    * <p>
    * This method should merge both the current version internally stored and the {@code remoteVersion} to achieve an
    * {@link IracEntryVersion} higher than both.
    *
    * @param segment       The segment.
    * @param remoteVersion The remote {@link IracEntryVersion} received.
    */
   void updateVersion(int segment, IracEntryVersion remoteVersion);

   /**
    * Stores a tombstone for a key removed.
    * <p>
    * It overwrites any existing tombstone.
    *
    * @param key      The key.
    * @param metadata The {@link IracMetadata}.
    */
   void storeTombstone(Object key, IracMetadata metadata);

   /**
    * Same as {@link #storeTombstone(Object, IracMetadata)} but it doesn't overwrite an existing tombstone.
    *
    * @param key      The key.
    * @param metadata The {@link IracMetadata}.
    */
   void storeTombstoneIfAbsent(Object key, IracMetadata metadata);

   /**
    * Returns the tombstone associated to the {@code key} or {@code null} if it doesn't exist.
    *
    * @param key The key.
    * @return The tombstone.
    */
   IracMetadata getTombstone(Object key);

   /**
    * Removes the tombstone for {@code key} if the metadata matches.
    *
    * @param key          The key.
    * @param iracMetadata The expected {@link IracMetadata}.
    */
   void removeTombstone(Object key, IracMetadata iracMetadata);

   /**
    * Removes the tombstone for {@code key}.
    *
    * @param key The key.
    */
   void removeTombstone(Object key);

   /**
    * Invoked when a topology change occurs in the cluster.
    *
    * @param newTopology The new {@link CacheTopology}
    */
   void onTopologyChange(CacheTopology newTopology);
}
