package org.infinispan.container.versioning.irac;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.topology.CacheTopology;

/**
 * A version generator for the IRAC protocol.
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
    * Same as {@link #generateNewMetadata(int)} but it makes sure the new version is higher than {@code versionSeen}.
    *
    * @param segment     The segment.
    * @param versionSeen The {@link IracEntryVersion} seen before. Can be {@code null}.
    * @return The {@link IracMetadata} created.
    */
   IracMetadata generateNewMetadata(int segment, IracEntryVersion versionSeen);

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
    * Invoked when a topology change occurs in the cluster.
    *
    * @param newTopology The new {@link CacheTopology}
    */
   void onTopologyChange(CacheTopology newTopology);
}
