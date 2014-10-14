package org.infinispan.cli.interpreter;

import org.infinispan.factories.components.ModuleMetadataFileFinder;

public class InterpreterMetadataFileFinder implements ModuleMetadataFileFinder {

   @Override
   public String getMetadataFilename() {
      return "infinispan-cli-server-component-metadata.dat";
   }

}
