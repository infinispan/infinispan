package org.infinispan.server.hotrod

import org.infinispan.factories.components.ModuleMetadataFileFinder

/**
 * @author gustavonalle
 * @since 8.0
 */
class ServerMetadataFileFinder extends ModuleMetadataFileFinder {
   def getMetadataFilename = "infinispan-server-hotrod-component-metadata.dat"
}
