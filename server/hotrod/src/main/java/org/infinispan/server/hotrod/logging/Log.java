package org.infinispan.server.hotrod.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

import java.util.Set;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.counter.exception.CounterException;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.server.hotrod.MissingFactoryException;
import org.infinispan.util.concurrent.IsolationLevel;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the Hot Rod server module. For this module, message ids ranging from 6001 to 7000 inclusively
 * have been reserved.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   @LogMessage(level = ERROR)
   @Message(value = "Exception reported", id = 5003)
   void exceptionReported(@Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "No members for new topology after applying consistent hash %s filtering into base topology %s", id = 5019)
   void noMembersInHashTopology(ConsistentHash ch, String topologyMap);

   @LogMessage(level = WARN)
   @Message(value = "No members in new topology", id = 5020)
   void noMembersInTopology();

   @LogMessage(level = WARN)
   @Message(value = "Server endpoint topology is empty, and cluster members are %s", id = 5021)
   void serverEndpointTopologyEmpty(String clusterMembers);

   @LogMessage(level = ERROR)
   @Message(value = "Exception writing response with messageId=%s", id = 5022)
   void errorWritingResponse(long msgId, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Exception encoding message %s", id = 5023)
   void errorEncodingMessage(Object msg, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Request to encode unexpected message %s", id = 5024)
   void errorUnexpectedMessage(Object msg);

   @LogMessage(level = WARN)
   @Message(value = "While trying to detect a crashed member, current view returned null", id = 6000)
   void viewNullWhileDetectingCrashedMember();

   @LogMessage(level = WARN)
   @Message(value = "Unable to update topology view after a crashed member left, wait for next view change.", id = 6001)
   void unableToUpdateView();

   @LogMessage(level = ERROR)
   @Message(value = "Error detecting crashed member", id = 6002)
   void errorDetectingCrashedMember(@Cause Throwable t);

   @Message(value = "A topology cache named '%s' has already been defined", id = 6003)
   CacheConfigurationException invalidTopologyCache(String topologyCacheName);

   @Message(value = "Cannot enable isolation level '%s' with writeSkewCheck disabled", id = 6004)
   CacheConfigurationException invalidIsolationLevel(IsolationLevel isolationLevel);

   @Message(value = "Cannot enable authentication without specifying a ServerAuthenticationProvider", id = 6005)
   CacheConfigurationException serverAuthenticationProvider();

   @Message(value = "The specified allowedMechs [%s] contains mechs which are unsupported by the underlying factories [%s]", id = 6006)
   CacheConfigurationException invalidAllowedMechs(Set<String> allowedMechs, Set<String> allMechs);

   @Message(value = "The requested operation is invalid", id = 6007)
   UnsupportedOperationException invalidOperation();

   @Message(value = "A serverName must be specified when enabling authentication", id = 6008)
   CacheConfigurationException missingServerName();

   @Message(value = "Event not handled by current Hot Rod event implementation: '%s'", id = 6009)
   IllegalStateException unexpectedEvent(Event e);

   @LogMessage(level = WARN)
   @Message(value = "Conditional operation '%s' should be used with transactional caches, otherwise data inconsistency issues could arise under failure situations", id = 6010)
   void warnConditionalOperationNonTransactional(String op);

   @LogMessage(level = WARN)
   @Message(value = "Operation '%s' forced to return previous value should be used on transactional caches, otherwise data inconsistency issues could arise under failure situations", id = 6011)
   void warnForceReturnPreviousNonTransactional(String op);

   @LogMessage(level = WARN)
   @Message(value = "Marshaller already set to '%s', ignoring passed '%s'", id = 6012)
   void warnMarshallerAlreadySet(Marshaller existingMarshaller, Marshaller newMarshaller);

   @Message(value = "Listener %s factory '%s' not found in server", id = 6013)
   MissingFactoryException missingCacheEventFactory(String factoryType, String name);

   @Message(value = "Trying to add a filter and converter factory with name '%s' but it does not extend CacheEventFilterConverterFactory", id = 6014)
   IllegalStateException illegalFilterConverterEventFactory(String name);

   @Message(value = "Failed iterating, invalid iteration id '%s' found", id = 6015)
   IllegalStateException illegalIterationId(String iterationId);

   @Message(value = "Factory '%s' not found in server", id = 6016)
   IllegalStateException missingKeyValueFilterConverterFactory(String name);

   @Message(value = "Unauthorized '%s' operation", id = 6017)
   SecurityException unauthorizedOperation(String op);

   @Message(value = "EXTERNAL SASL mechanism not allowed without SSL client certificate", id = 6018)
   SecurityException externalMechNotAllowedWithoutSSLClientCert();

   @Message(value = "A host or proxyHost address has not been specified", id = 6019)
   CacheConfigurationException missingHostAddress();

   @Message(value = "Cache '%s' is not transactional to execute a client transaction", id = 6020)
   IllegalStateException expectedTransactionalCache(String cacheName);

   @Message(value = "Cache '%s' must have REPEATABLE_READ isolation level", id = 6021)
   IllegalStateException unexpectedIsolationLevel(String cacheName);

   @Message(value = "Expects a STRONG counter for '%s'", id = 28023)
   CounterException invalidWeakCounter(String name);

   @LogMessage(level = WARN)
   @Message(value = "Not wrapping custom marshaller with media type '%s' since the format is already supported by the server", id = 28024)
   void skippingMarshallerWrapping(String mediaType);

   @Message(value = "Error serializing script response '%s'", id = 28025)
   EncodingException errorSerializingResponse(Object o);
}
