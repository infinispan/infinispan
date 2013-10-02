package org.infinispan.stats;

import org.infinispan.factories.components.ModuleMetadataFileFinder;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */
public class ExtendedStatisticsMetadataFileFinder implements ModuleMetadataFileFinder {
   @Override
   public String getMetadataFilename() {
      return "infinispan-extended-statistics-component-metadata.dat";
   }
}
