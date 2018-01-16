package org.infinispan.cli.interpreter;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

@MetaInfServices
@SuppressWarnings("unused")
public class InterpreterMetadataFileFinder implements ModuleMetadataFileFinder {

   @Override
   public String getMetadataFilename() {
      return "infinispan-cli-interpreter-component-metadata.dat";
   }

}
