package org.infinispan.lock.impl;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

/**
 * Metadata file name, needed for dependency injection.
 *
 * @author Katia Aresti, karesti@redhat.com
 * @since 9.2
 */
@MetaInfServices
public class ClusteredLockMetadataFileFinder implements ModuleMetadataFileFinder {

   @Override
   public String getMetadataFilename() {
      return "infinispan-clustered-lock-component-metadata.dat";
   }
}
