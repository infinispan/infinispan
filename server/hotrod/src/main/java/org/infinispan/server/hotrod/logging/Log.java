package org.infinispan.server.hotrod.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

import java.lang.invoke.MethodHandles;
import java.net.SocketAddress;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.server.hotrod.MissingFactoryException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.Once;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log abstraction for the Hot Rod server module. For this module, message ids ranging from 6001 to 7000 inclusively
 * have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 6001, max = 7000)
public interface Log extends BasicLogger {
   Log CONFIG = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, "org.infinispan.CONFIG");

   static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(MethodHandles.lookup(), Log.class, clazz.getName());
   }

   @LogMessage(level = ERROR)
   @Message(value = "Error detecting crashed member", id = 6002)
   void errorDetectingCrashedMember(@Cause Throwable t);

   @Message(value = "The requested operation is invalid", id = 6007)
   UnsupportedOperationException invalidOperation();

   @Message(value = "Event not handled by current Hot Rod event implementation: '%s'", id = 6009)
   IllegalStateException unexpectedEvent(Event e);

   @LogMessage(level = WARN)
   @Message(value = "Conditional operation '%s' should be used with transactional caches, otherwise data inconsistency issues could arise under failure situations", id = 6010)
   @Once
   void warnConditionalOperationNonTransactional(String op);

   @LogMessage(level = WARN)
   @Message(value = "Operation '%s' forced to return previous value should be used on transactional caches, otherwise data inconsistency issues could arise under failure situations", id = 6011)
   @Once
   void warnForceReturnPreviousNonTransactional(String op);

   @Message(value = "Listener %s factory '%s' not found in server", id = 6013)
   MissingFactoryException missingCacheEventFactory(String factoryType, String name);

   @Message(value = "Trying to add a filter and converter factory with name '%s' but it does not extend CacheEventFilterConverterFactory", id = 6014)
   IllegalStateException illegalFilterConverterEventFactory(String name);

   /*
     Moved to server-core

   @Message(value = "Factory '%s' not found in server", id = 6016)
   IllegalStateException missingKeyValueFilterConverterFactory(String name);
   */

   @Message(value = "Operation '%s' requires authentication", id = 6017)
   SecurityException unauthorizedOperation(String op);

   @Message(value = "A host or proxyHost address has not been specified", id = 6019)
   CacheConfigurationException missingHostAddress();

   @Message(value = "Cache '%s' is not transactional to execute a client transaction", id = 6020)
   IllegalStateException expectedTransactionalCache(String cacheName);

   @Message(value = "Cache '%s' must have REPEATABLE_READ isolation level", id = 6021)
   IllegalStateException unexpectedIsolationLevel(String cacheName);

   @LogMessage(level = WARN)
   @Message(value = "Not wrapping custom marshaller with media type '%s' since the format is already supported by the server", id = 6022)
   @Once
   void skippingMarshallerWrapping(String mediaType);

   @Message(value = "Error serializing script response '%s'", id = 6023)
   EncodingException errorSerializingResponse(Object o);

//   @LogMessage(level = WARN)
//   @Message(value = "Removed unclosed iterator '%s'", id = 28026)
//   void removedUnclosedIterator(String iteratorId);

   @Message(value = "Invalid credentials", id = 6024)
   SecurityException authenticationException(@Cause Throwable cause);

   @Message(value = "Invalid mech '%s'", id = 6025)
   IllegalArgumentException invalidMech(String mech);

   @LogMessage(level = WARN)
   @Message(value = "Client %s keeps providing outdated topology %s", id = 6026)
   void clientNotUpdatingTopology(SocketAddress socketAddress, int topologyId);
}
