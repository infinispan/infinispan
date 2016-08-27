package org.infinispan.server.core;

import org.infinispan.factories.components.ModuleMetadataFileFinder;

public class ServerCoreMetadataFileFinder implements ModuleMetadataFileFinder {
   public String getMetadataFilename() {
      return "infinispan-server-core-component-metadata.dat";
   }
}
