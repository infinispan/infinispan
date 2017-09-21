package org.infinispan.stats;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

/**
 * @author Pedro Ruivo
 * @since 6.0
 */

@MetaInfServices
@SuppressWarnings("unused")
public class ExtendedStatisticsMetadataFileFinder implements ModuleMetadataFileFinder {
   @Override
   public String getMetadataFilename() {
      return "infinispan-extended-statistics-component-metadata.dat";
   }
}
