package org.infinispan.query.impl;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

@MetaInfServices
public class QueryMetadataFileFinder implements ModuleMetadataFileFinder {
   @Override
   public String getMetadataFilename() {
      return "infinispan-query-component-metadata.dat";
   }
}
