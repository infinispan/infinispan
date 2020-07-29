package org.infinispan.xsite.irac;

import org.infinispan.container.versioning.irac.IracEntryVersion;
import org.infinispan.container.versioning.irac.IracVersionGenerator;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.topology.CacheTopology;

/**
 * An {@link IracVersionGenerator} implementation that can be controlled for testing.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
public class ControlledIracVersionGenerator implements IracVersionGenerator {

   private final IracVersionGenerator actual;

   public ControlledIracVersionGenerator(IracVersionGenerator actual) {
      this.actual = actual;
   }

   @Override
   public IracMetadata generateNewMetadata(int segment) {
      return actual.generateNewMetadata(segment);
   }

   @Override
   public IracMetadata generateMetadataWithCurrentVersion(int segment) {
      return actual.generateMetadataWithCurrentVersion(segment);
   }

   @Override
   public IracMetadata mergeVersion(int segment, IracEntryVersion localVersion, IracEntryVersion remoteVersion,
         String siteName) {
      return actual.mergeVersion(segment, localVersion, remoteVersion, siteName);
   }

   @Override
   public void updateVersion(int segment, IracEntryVersion remoteVersion) {
      actual.updateVersion(segment, remoteVersion);
   }

   @Override
   public void storeTombstone(Object key, IracMetadata metadata) {
      actual.storeTombstone(key, metadata);
   }

   @Override
   public void storeTombstoneIfAbsent(Object key, IracMetadata metadata) {
      actual.storeTombstoneIfAbsent(key, metadata);
   }

   @Override
   public IracMetadata getTombstone(Object key) {
      return actual.getTombstone(key);
   }

   @Override
   public void removeTombstone(Object key, IracMetadata iracMetadata) {
      actual.removeTombstone(key, iracMetadata);
   }

   @Override
   public void removeTombstone(Object key) {
      actual.removeTombstone(key);
   }

   @Override
   public void onTopologyChange(CacheTopology newTopology) {
      actual.onTopologyChange(newTopology);
   }

   @Override
   public void start() {
      actual.start();
   }

   @Override
   public void stop() {
      actual.stop();
   }
}
