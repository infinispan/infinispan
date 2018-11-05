package org.infinispan.rest.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.TRACE;
import static org.jboss.logging.Logger.Level.WARN;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.rest.cachemanager.exceptions.CacheUnavailableException;
import org.infinispan.rest.framework.Method;
import org.infinispan.rest.framework.RegistrationException;
import org.infinispan.rest.operations.exceptions.NoCacheFoundException;
import org.infinispan.rest.operations.exceptions.ServiceUnavailableException;
import org.infinispan.rest.operations.exceptions.UnacceptableDataFormatException;
import org.jboss.logging.BasicLogger;
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
   @Message(value = "Error transcoding content", id = 495)
   EncodingException errorTranscoding(@Cause Throwable cause);

   @Message(value = "Unsupported configuration option", id = 12004)
   UnsupportedOperationException unsupportedConfigurationOption();

   @LogMessage(level = TRACE)
   @Message(value = "An error occurred while responding to the client", id = 12005)
   void errorWhileResponding(@Cause Exception e);

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
   RegistrationException duplicateResource(String invocationName, Method method, String existingPath);

   @LogMessage(level = WARN)
   @Message(value = "Header '%s' will be ignored, expecting a number but got '%s'", id = 12016)
   void warnInvalidNumber(String header, String value);
}
