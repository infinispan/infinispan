package org.infinispan.multimap.impl;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

/**
 * Multimap Cache module implementation of {@link ModuleMetadataFileFinder}
 *
 * @author Katia Aresti - karesti@redhat.com
 * @since 9.2
 */
@MetaInfServices
public class EmbeddedMultimapMetadataFileFinder implements ModuleMetadataFileFinder {

   @Override
   public String getMetadataFilename() {
      return "infinispan-multimap-component-metadata.dat";
   }
}
