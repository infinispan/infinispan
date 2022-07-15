package org.infinispan.multimap.logging;

import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Range: 31001 - 31500
 *
 * @since 15.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log {

   String LOG_ROOT = "org.infinispan";
   Log CONTAINER = Logger.getMessageLogger(Log.class, LOG_ROOT + ".CONTAINER");

   @Message(value = "The multimap name is missing.", id = 31001)
   IllegalArgumentException missingMultimapName();

   @Message(value = "Duplicated name '%s' for multimap", id = 31002)
   IllegalArgumentException duplicatedMultimapName(String name);

   @Message(value = "Configuration not defined for multimap '%s'", id = 31003)
   IllegalArgumentException undefinedConfiguration(String name);
}
