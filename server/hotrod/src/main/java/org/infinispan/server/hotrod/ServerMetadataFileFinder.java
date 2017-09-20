package org.infinispan.server.hotrod;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.infinispan.lifecycle.ModuleLifecycle;
import org.kohsuke.MetaInfServices;

/**
 * @author gustavonalle
 * @since 8.0
 */
@MetaInfServices(ModuleLifecycle.class)
public final class ServerMetadataFileFinder implements ModuleMetadataFileFinder {
   @Override
   public String getMetadataFilename() {
      return "infinispan-server-hotrod-component-metadata.dat";
   }
}
