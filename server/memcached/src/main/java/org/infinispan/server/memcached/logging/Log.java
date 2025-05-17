package org.infinispan.server.memcached.logging;

import static org.jboss.logging.Logger.Level.WARN;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log abstraction for the Memcached server module.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 11001, max = 12000)
public interface Log extends BasicLogger {
   Log SERVER = Logger.getMessageLogger(Log.class, "org.infinispan.SERVER");

//   @LogMessage(level = ERROR)
//   @Message(value = "Exception reported", id = 5003)
//   void exceptionReported(@Cause Throwable t);

//   @Message(value = "Cache '%s' has expiration enabled which violates the Memcached protocol", id = 11001)
//   CacheConfigurationException invalidExpiration(String cacheName);

//   @Message(value = "Cannot enable Memcached text-protocol detection when authentication is disabled", id = 11002)
//   CacheConfigurationException cannotDetectMemcachedTextWithoutAuthentication();

   @LogMessage(level = WARN)
   @Message(value = "Received an unexpected exception.", id = 11003)
   void unexpectedException(@Cause Throwable cause);
}
