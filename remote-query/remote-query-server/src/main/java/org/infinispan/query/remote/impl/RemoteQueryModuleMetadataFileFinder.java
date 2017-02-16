package org.infinispan.query.remote.impl;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
@MetaInfServices
public final class RemoteQueryModuleMetadataFileFinder implements ModuleMetadataFileFinder {

   @Override
   public String getMetadataFilename() {
      return "infinispan-remote-query-server-component-metadata.dat";
   }
}
