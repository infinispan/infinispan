package org.infinispan.rest.logging;

import static org.jboss.logging.Logger.Level.ERROR;

import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the REST server module. For this module, message ids
 * ranging from 12001 to 13000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface JavaLog extends org.infinispan.util.logging.Log {

   @LogMessage(level = ERROR)
   @Message(value = "Error reading configuration file for REST server: %s", id = 12001)
   void errorReadingConfigurationFile(@Cause Throwable t, String path);

   @LogMessage(level = ERROR)
   @Message(value = "Error while retrieving cache manager from JBoss Microcontainer", id = 12002)
   void errorRetrievingCacheManagerFromMC(@Cause Throwable t);
}
