package org.infinispan.server.eventlogger.impl;

import org.infinispan.factories.components.ModuleMetadataFileFinder;
import org.kohsuke.MetaInfServices;

/**
 * ServerEventLoggerMetadataFileFinder.
 *
 * @author Tristan Tarrant
 * @since 8.2
 */
@MetaInfServices
public class ServerEventLoggerMetadataFileFinder implements ModuleMetadataFileFinder {
   @Override
   public String getMetadataFilename() {
      return "infinispan-server-event-logger-component-metadata.dat";
   }
}
