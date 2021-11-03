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
   public IracMetadata generateNewMetadata(int segment, IracEntryVersion versionSeen) {
      return actual.generateNewMetadata(segment, versionSeen);
   }

   @Override
   public void updateVersion(int segment, IracEntryVersion remoteVersion) {
      actual.updateVersion(segment, remoteVersion);
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
