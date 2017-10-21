package org.infinispan.rest.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.TRACE;
import static org.jboss.logging.Logger.Level.WARN;

import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the REST server module. For this module, message ids ranging from 12001 to 13000 inclusively have
 * been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {

   @LogMessage(level = ERROR)
   @Message(value = "Error reading configuration file for REST server: %s", id = 12001)
   void errorReadingConfigurationFile(@Cause Throwable t, String path);

   @LogMessage(level = ERROR)
   @Message(value = "Error while retrieving cache manager from JBoss Microcontainer", id = 12002)
   void errorRetrievingCacheManagerFromMC(@Cause Throwable t);

   @LogMessage(level = INFO)
   @Message(value = "REST server starting, listening on %s:%s", id = 12003)
   void startRestServer(String host, int port);

   @Message(value = "Unsupported configuration option", id = 12004)
   UnsupportedOperationException unsupportedConfigurationOption();

   @LogMessage(level = TRACE)
   @Message(value = "An error occurred while responding to the client", id = 12005)
   void errorWhileResponding(@Cause Exception e);

   @LogMessage(level = WARN)
   @Message(value = "Uncaught exception in the pipeline", id = 12006)
   void uncaughtExceptionInThePipeline(@Cause Throwable e);


}
