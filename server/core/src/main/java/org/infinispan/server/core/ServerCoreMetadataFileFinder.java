package org.infinispan.server.core;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

@MetaInfServices
@SuppressWarnings("unused")
public final class ServerCoreMetadataFileFinder implements ModuleMetadataFileFinder {
   public String getMetadataFilename() {
      return "infinispan-server-core-component-metadata.dat";
   }
}
