package org.infinispan.server.core

import org.infinispan.factories.components.ModuleMetadataFileFinder

class ServerCoreMetadataFileFinder extends ModuleMetadataFileFinder {
   def getMetadataFilename = "infinispan-server-core-component-metadata.dat"
}