package org.infinispan.server.resp.logging;

import org.infinispan.commons.CacheConfigurationException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.Once;

import java.nio.CharBuffer;

import static org.jboss.logging.Logger.Level.WARN;

/**
 * Log abstraction for the Resp protocol server module. For this module, message ids
 * ranging from 12001 to 13000 inclusively have been reserved.
 *
 * @author William Burns
 * @since 14.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   Log CONFIG = Logger.getMessageLogger(Log.class, "org.infinispan.CONFIG");
   Log SERVER = Logger.getMessageLogger(Log.class, "org.infinispan.SERVER");

//   @Message(value = "Cache '%s' has expiration enabled which violates the RESP protocol.", id = 12001)
//   CacheConfigurationException invalidExpiration(String cacheName);

   @Message(value = "Cannot enable authentication without an authenticator.", id = 12002)
   CacheConfigurationException authenticationWithoutAuthenticator();

   @LogMessage(level = WARN)
   @Message(value = "Received an unexpected exception.", id = 12003)
   void unexpectedException(@Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "An error occurred when removing the listener for channel %s", id = 12004)
   void exceptionWhileRemovingListener(@Cause Throwable cause, CharBuffer channelName);

   @LogMessage(level = WARN)
   @Message(value = "There was an error adding listener for channel %s", id = 12005)
   void exceptionWhileRegisteringListener(@Cause Throwable cause, CharBuffer channelName);

   @LogMessage(level = WARN)
   @Message(value = "LMOVE command can't guarantee atomicity and consistency when the source list and the destination list are different", id = 12006)
   @Once
   void lmoveConsistencyMessage();

   @Once
   @LogMessage(level = WARN)
   @Message(value = "Multi-key operations without batching have a relaxed isolation level. Consider enabling batching.", id = 12007)
   void multiKeyOperationUseBatching();

   @LogMessage(level = WARN)
   @Message(value = "SMOVE command can't guarantee atomicity and consistency when the source list and the destination set are different", id = 12008)
   @Once
   void smoveConsistencyMessage();

   @LogMessage(level = WARN)
   @Message(value = "MSETNX command can't guarantee atomicity and consistency against concurrent set", id = 12009)
   @Once
   void msetnxConsistencyMessage();

}
