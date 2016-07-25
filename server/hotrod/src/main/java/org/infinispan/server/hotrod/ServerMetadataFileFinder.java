package org.infinispan.server.hotrod;

import org.infinispan.factories.components.ModuleMetadataFileFinder;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class ServerMetadataFileFinder implements ModuleMetadataFileFinder {
   @Override
   public String getMetadataFilename() {
      return "infinispan-server-hotrod-component-metadata.dat";
   }
}
