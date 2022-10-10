package org.infinispan.server.memcached.logging;

import static org.jboss.logging.Logger.Level.ERROR;

import org.infinispan.commons.CacheConfigurationException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the Memcached server module. For this module, message ids
 * ranging from 11001 to 12000 inclusively have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   @LogMessage(level = ERROR)
   @Message(value = "Exception reported", id = 5003)
   void exceptionReported(@Cause Throwable t);

   @Message(value = "Cache '%s' has expiration enabled which violates the Memcached protocol", id = 11001)
   CacheConfigurationException invalidExpiration(String cacheName);
}
