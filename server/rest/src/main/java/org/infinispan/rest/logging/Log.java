package org.infinispan.rest.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import java.io.IOException;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.rest.cachemanager.exceptions.CacheUnavailableException;
import org.infinispan.rest.framework.Invocation;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RegistrationException;
import org.infinispan.rest.operations.exceptions.NoCacheFoundException;
import org.infinispan.rest.operations.exceptions.ServiceUnavailableException;
import org.infinispan.rest.operations.exceptions.UnacceptableDataFormatException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
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
public interface Log extends BasicLogger {
   Log REST = Logger.getMessageLogger(Log.class, org.infinispan.util.logging.Log.LOG_ROOT + "REST");

//   @Message(value = "Error transcoding content", id = 495)
//   EncodingException errorTranscoding(@Cause Throwable cause);

   @Message(value = "Unsupported configuration option", id = 12004)
   UnsupportedOperationException unsupportedConfigurationOption();

   @LogMessage(level = ERROR)
   @Message(value = "An error occurred while responding to the client", id = 12005)
   void errorWhileResponding(@Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Uncaught exception in the pipeline", id = 12006)
   void uncaughtExceptionInThePipeline(@Cause Throwable e);

   @Message(value = "Cannot convert to %s", id = 12007)
   UnacceptableDataFormatException unsupportedDataFormat(String mediaType);

   @Message(value = "Cache with name '%s' is temporarily unavailable.", id = 12008)
   ServiceUnavailableException cacheUnavailable(String cacheName);

   @Message(value = "Cannot obtain cache '%s', without required MediaType", id = 12009)
   NullPointerException missingRequiredMediaType(String cacheName);

   @Message(value = "Cache with name '%s' not found amongst the configured caches", id = 12010)
   NoCacheFoundException cacheNotFound(String cacheName);

   @Message(value = "Remote requests are not allowed to private caches. Do no send remote requests to cache '%s'", id = 12011)
   CacheUnavailableException requestNotAllowedToInternalCaches(String cacheName);

   @Message(value = "Remote requests are not allowed to internal caches when authorization is disabled. Do no send remote requests to cache '%s'", id = 12012)
   CacheUnavailableException requestNotAllowedToInternalCachesWithoutAuthz(String cacheName);

   @Message(value = "Illegal compression level '%d'. The value must be >= 0 and <= 9", id = 12014)
   CacheConfigurationException illegalCompressionLevel(int compressionLevel);

   @Message(value = "Cannot register invocation '%s': resource already registered for method '%s' at the destination path '/%s'", id = 12015)
   RegistrationException duplicateResourceMethod(String invocationName, Method method, String existingPath);

   @LogMessage(level = WARN)
   @Message(value = "Header '%s' will be ignored, expecting a number but got '%s'", id = 12016)
   void warnInvalidNumber(String header, String value);

   @Message(value = "Cannot enable authentication without an authenticator", id = 12017)
   CacheConfigurationException authenticationWithoutAuthenticator();

   @Message(value = "Cannot register invocation with path '%s': '*' is only allowed at the end", id = 12018)
   RegistrationException invalidPath(String path);

   @Message(value = "Cannot register path '%s' for invocation '%s', since it conflicts with resource '%s'", id = 12019)
   RegistrationException duplicateResource(String candidate, Invocation invocation, String existingPath);

   @LogMessage(level = INFO)
   @Message(value = "MassIndexer started", id = 12020)
   void asyncMassIndexerStarted();

   @LogMessage(level = INFO)
   @Message(value = "MassIndexer completed successfully", id = 12021)
   void asyncMassIndexerSuccess();

   @LogMessage(level = ERROR)
   @Message(value = "Error executing MassIndexer", id = 12022)
   void errorExecutingMassIndexer(@Cause Throwable e);

   @Message(value = "Argument '%s' has illegal value '%s'", id = 12023)
   IllegalArgumentException illegalArgument(String name, Object value);

   @Message(value = "Synchronized %d entries", id = 12024)
   String synchronizedEntries(long hotrod);

   @LogMessage(level = WARN)
   @Message(value = "Ignoring invalid origin '%s' when reading '-D%s'", id = 12025)
   void invalidOrigin(String origin, String prop);

   @LogMessage(level = WARN)
   @Message(value = "The REST invocation [%s] has been deprecated. Please consult the upgrade guide", id = 12026)
   void warnDeprecatedCall(String invocation);

   @Message(value = "Security authorization is not enabled on this server.", id = 12027)
   String authorizationNotEnabled();

   @Message(value = "The principal-role mapper is not mutable", id = 12028)
   String principalRoleMapperNotMutable();

   @Message(value = "The role-permission mapper is not mutable", id = 12029)
   String rolePermissionMapperNotMutable();

   @Message(value = "Heap dump generation failed", id = 12030)
   RuntimeException heapDumpFailed(@Cause IOException e);
}
