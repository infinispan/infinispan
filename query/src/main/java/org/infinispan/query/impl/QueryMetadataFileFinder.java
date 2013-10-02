package org.infinispan.query.impl;

import org.infinispan.factories.components.ModuleMetadataFileFinder;

public class QueryMetadataFileFinder implements ModuleMetadataFileFinder {
   @Override
   public String getMetadataFilename() {
      return "infinispan-query-component-metadata.dat";
   }
}
