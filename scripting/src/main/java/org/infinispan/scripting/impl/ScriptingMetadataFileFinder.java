package org.infinispan.scripting.impl;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

/**
 * ScriptingMetadataFileFinder.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@MetaInfServices
public class ScriptingMetadataFileFinder implements ModuleMetadataFileFinder {
   @Override
   public String getMetadataFilename() {
      return "infinispan-scripting-component-metadata.dat";
   }
}
