package org.infinispan.tasks.impl;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

/**
 * TasksMetadataFileFinder.
 *
 * @author Tristan Tarrant
 * @since 8.1
 */
@MetaInfServices
@SuppressWarnings("unused")
public class TasksMetadataFileFinder implements ModuleMetadataFileFinder {
   @Override
   public String getMetadataFilename() {
      return "infinispan-tasks-component-metadata.dat";
   }
}
