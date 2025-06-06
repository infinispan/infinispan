package org.infinispan.commons.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.INFO;
import static org.jboss.logging.Logger.Level.WARN;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.time.Instant;
import java.util.EnumSet;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.EncodingException;
import org.infinispan.commons.dataconversion.MediaType;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.Suppressed;
import org.jboss.logging.annotations.ValidIdRange;
import org.jboss.logging.annotations.ValidIdRanges;

/**
 * Infinispan's log abstraction layer on top of JBoss Logging.
  * It contains explicit methods for all INFO or above levels so that they can
 * be internationalized. For the commons module, message ids ranging from 0901
 * to 1000 inclusively have been reserved.
  * <code> Log log = LogFactory.getLog( getClass() ); </code> The above will get
 * you an instance of <code>Log</code>, which can be used to generate log messages
 * either via JBoss Logging which then can delegate to Log4J (if the libraries
 * are present) or (if not) the built-in JDK logger.
  * In addition to the 6 log levels available, this framework also supports
 * parameter interpolation, similar to the JDKs {@link String#format(String, Object...)}
 * method. What this means is, that the following block:
 * <code> if (log.isTraceEnabled()) { log.trace("This is a message " + message + " and some other value is " + value); }
 * </code>
  * ... could be replaced with ...
  * <code> if (log.isTraceEnabled()) log.tracef("This is a message %s and some other value is %s", message, value);
 * </code>
  * This greatly enhances code readability.
  * If you are passing a <code>Throwable</code>, note that this should be passed in
 * <i>before</i> the vararg parameter list.
  *
 * @author Manik Surtani
 * @since 4.0
 * @api.private
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRanges({
      @ValidIdRange(min = 901, max = 1000),
      @ValidIdRange(min = 29501, max = 29600) // To be moved
})
public interface Log extends BasicLogger {
   String LOG_ROOT = "org.infinispan.";
   Log CONFIG = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, LOG_ROOT + "CONFIG");
   Log CONTAINER = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, LOG_ROOT + "CONTAINER");
   Log SECURITY = Logger.getMessageLogger(MethodHandles.lookup(), Log.class, LOG_ROOT + "SECURITY");

   @LogMessage(level = WARN)
   @Message(value = "Property %s could not be replaced as intended!", id = 901)
   void propertyCouldNotBeReplaced(String line);

   @LogMessage(level = WARN)
   @Message(value = "Invocation of %s threw an exception %s. Exception is ignored.", id = 902)
   void ignoringException(String methodName, String exceptionName, @Cause Throwable t);

//   @LogMessage(level = ERROR)
//   @Message(value = "Unable to set value!", id = 903)
//   void unableToSetValue(@Cause Exception e);

   @Message(value = "Error while initializing SSL context", id = 904)
   CacheConfigurationException sslInitializationException(@Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to load %s from any of the following classloaders: %s", id = 905)
   void unableToLoadClass(String classname, String classloaders, @Cause Throwable cause);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to an int! Using default value of %d", id = 906)
   void unableToConvertStringPropertyToInt(String value, int defaultValue);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to a long! Using default value of %d", id = 907)
   void unableToConvertStringPropertyToLong(String value, long defaultValue);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert string property [%s] to a boolean! Using default value of %b", id = 908)
   void unableToConvertStringPropertyToBoolean(String value, boolean defaultValue);

   @Message(value = "Unwrapping %s to a type of %s is not a supported", id = 909)
   IllegalArgumentException unableToUnwrap(Object o, Class<?> clazz);

   @Message(value = "Illegal value for thread pool parameter(s) %s, it should be: %s", id = 910)
   CacheConfigurationException illegalValueThreadPoolParameter(String parameter, String requirement);

   @Message(value = "Unwrapping of any instances in %s to a type of %s is not a supported", id = 911)
   IllegalArgumentException unableToUnwrapAny(String objs, Class<?> clazz);

   @Message(value = "Expecting a protected configuration for %s", id = 912)
   IllegalStateException unprotectedAttributeSet(String name);

   @Message(value = "Expecting an unprotected configuration for %s", id = 913)
   IllegalStateException protectedAttributeSet(String name);

   @Message(value = "Duplicate attribute '%s' in attribute set '%s'", id = 914)
   IllegalArgumentException attributeSetDuplicateAttribute(String name, String setName);

   @Message(value = "No such attribute '%s' in attribute set '%s'", id = 915)
   IllegalArgumentException noSuchAttribute(String name, String setName);

   @Message(value = "No attribute copier for type '%s'", id = 916)
   IllegalArgumentException noAttributeCopierForType(Class<?> klass);

//   @Message(value = "Cannot resize unbounded container", id = 917)
//   UnsupportedOperationException cannotResizeUnboundedContainer();

   @Message(value = "Cannot find resource '%s'", id = 918)
   IOException cannotFindResource(String fileName);

   @Message(value = "Multiple errors encountered while validating configuration", id = 919)
   CacheConfigurationException multipleConfigurationValidationErrors();

   @Message(value = "Unable to load file using scheme %s", id = 920)
   UnsupportedOperationException unableToLoadFileUsingScheme(String scheme);

   @Message(value = "The alias '%s' does not exist in the key store '%s'", id = 921)
   SecurityException noSuchAliasInKeyStore(String keyAlias, String keyStoreFileName);

   @LogMessage(level = ERROR)
   @Message(value = "Exception during rollback", id = 922)
   void errorRollingBack(@Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "Error enlisting resource", id = 923)
   void errorEnlistingResource(@Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "beforeCompletion() failed for %s", id = 924)
   void beforeCompletionFailed(String synchronization, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Unexpected error from resource manager!", id = 925)
   void unexpectedErrorFromResourceManager(@Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "afterCompletion() failed for %s", id = 926)
   void afterCompletionFailed(String synchronization, @Cause Throwable t);

   @LogMessage(level = WARN)
   @Message(value = "exception while committing", id = 927)
   void errorCommittingTx(@Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "end() failed for %s", id = 928)
   void xaResourceEndFailed(String xaResource, @Cause Throwable t);

   @Message(value = "Media type cannot be empty or null!", id = 929)
   EncodingException missingMediaType();

   @Message(value = "Invalid media type '%s': must contain a type and a subtype separated by '/'", id = 930)
   EncodingException invalidMediaTypeSubtype(String mediaType);

   @Message(value = "Invalid media type '%s': invalid param '%s'", id = 931)
   EncodingException invalidMediaTypeParam(String mediaType, String param);

//   @Message(value = "Invalid media type list '%s': comma expected", id = 932)
//   EncodingException invalidMediaTypeListCommaMissing(String mediaType);

   @Message(value = "Invalid media type list '%s': type expected after comma", id = 933)
   EncodingException invalidMediaTypeListCommaAtEnd(String mediaType);

   //
   @Message(value = "Errors converting '%s' from '%s' to '%s'", id = 934)
   EncodingException errorTranscoding(String content, MediaType contentType, MediaType requestType, @Cause Throwable t);

   @Message(value = "Invalid Weight '%s'. Supported values are between 0 and 1.0", id = 935)
   EncodingException invalidWeight(Object weight);

   @Message(value = "Class '%s' blocked by deserialization allow list. Adjust the configuration serialization allow list regular expression to include this class.", id = 936)
   CacheException classNotInAllowList(String className);

   @Message(value = "Invalid media type. Expected '%s' but got '%s'", id = 937)
   EncodingException invalidMediaType(String expected, String actual);

   @Message(value = "Invalid text content '%s'", id = 938)
   EncodingException invalidTextContent(Object content);

   @Message(value = "Conversion of content '%s' from '%s' to '%s' not supported", id = 939)
   EncodingException conversionNotSupported(Object content, String fromMediaType, String toMediaType);

   @Message(value = "Invalid application/x-www-form-urlencoded content: '%s'", id = 940)
   EncodingException cannotDecodeFormURLContent(Object content);

   @Message(value = "Error encoding content '%s' to '%s'", id = 941)
   EncodingException errorEncoding(Object content, MediaType mediaType);

   @LogMessage(level = WARN)
   @Message(value = "Unable to convert property [%s] to an enum! Using default value of %d", id = 942)
   void unableToConvertStringPropertyToEnum(String value, String defaultValue);

//   @LogMessage(level = ERROR)
//   @Message(value = "Could not register object with name: %s", id = 943)
//   void couldNotRegisterObjectName(ObjectName objectName, @Cause Exception e);

   @Message(value = "Feature %s is disabled!", id = 944)
   CacheConfigurationException featureDisabled(String feature);

//   @Message(value = "Unable to marshall Object '%s' wrapped by '%s', the wrapped object must be registered with the marshallers SerializationContext", id = 945)
//   MarshallingException unableToMarshallRuntimeObject(String wrappedObjectClass, String wrapperClass);

//   @LogMessage(level = INFO)
//   @Message(value = "Using OpenSSL Provider", id = 946)
//   void openSSLAvailable();
//
//   @LogMessage(level = INFO)
//   @Message(value = "Using Java SSL Provider", id = 947)
//   void openSSLNotAvailable();

   @Message(value = "Unsupported conversion of '%s' from '%s' to '%s'", id = 948)
   EncodingException unsupportedConversion(String content, MediaType contentType, MediaType requestType);

   @Message(value = "Unsupported conversion of '%s' to '%s'", id = 949)
   EncodingException unsupportedConversion(String content, MediaType requestType);

   @Message(value = "Encoding '%s' is not supported", id = 950)
   EncodingException encodingNotSupported(String enc);

   @Message(value = "Invalid value %s for attribute %s: must be a number greater than zero", id = 951)
   CacheConfigurationException attributeMustBeGreaterThanZero(Number value, Enum<?> attribute);

   @LogMessage(level = INFO)
   @Message(value = "OpenTelemetry tracing instance loaded: %s", id = 952)
   void telemetryLoaded(Object telemetry);

//   @LogMessage(level = INFO)
//   @Message(value = "OpenTelemetry tracing integration is disabled", id = 953)
//   void telemetryDisabled();

   @LogMessage(level = WARN)
   @Message(value = "OpenTelemetry tracing cannot be configured.", id = 954)
   void errorOnLoadingTelemetry(@Cause Throwable t);

   @Message(value = "'%s' is not a valid boolean value (true|false|yes|no|y|n|on|off)", id = 955)
   IllegalArgumentException illegalBooleanValue(String value);

   @Message(value = "'%s' is not one of %s", id = 956)
   IllegalArgumentException illegalEnumValue(String value, EnumSet<?> set);

   @LogMessage(level = ERROR)
   @Message(value = "Cannot load %s", id = 957)
   void cannotLoadMimeTypes(String mimeTypes);

   @Message(value = "Cannot parse bytes quantity %s", id = 958)
   IllegalArgumentException cannotParseQuantity(String str);

   @LogMessage(level = WARN)
   @Message(value = "Property '%s' has been deprecated. Please use '%s' instead.", id = 959)
   void deprecatedProperty(String oldName, String newName);

   @Message(value = "No attribute '%s' in '%s'", id = 960)
   IllegalArgumentException noAttribute(String name, String element);

   @Message(value = "Incompatible attribute '%s.%s' existing value='%s', new value='%s'", id = 961)
   IllegalArgumentException incompatibleAttribute(String parentName, String name, String v1, String v2);

   @Message(value = "Cannot modify protected attribute '%s'", id = 962)
   IllegalStateException protectedAttribute(String name);

   @Message(value = "Invalid configuration in '%s'", id = 963)
   IllegalArgumentException invalidConfiguration(String name);

   @Message(value = "RESP cache '%s' key media type must be configured as application/octet-stream but was %s", id = 964)
   IllegalArgumentException respCacheKeyMediaTypeSupplied(String cacheName, MediaType mediaType);

   @Message(value = "Relation between segment and slots only for power 2, received: %s", id = 965)
   IllegalArgumentException respCacheSegmentSizePow2(int configured);

   @Message(value = "RESP cache '%s' should use RESPHashFunctionPartitioner but is using %s", id = 966)
   IllegalArgumentException respCacheUseDefineConsistentHash(String cacheName, String configured);

   @Message(value = "Cannot parse Prometheus URL for tracing.'", id = 967)
   CacheConfigurationException errorOnParsingPrometheusURLForTracing(@Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Failed creating initial JNDI context", id = 968)
   void failedToCreateInitialCtx(@Cause Throwable e);

   @Message(value = "Error while reloading certificate", id = 970)
   RuntimeException certificateReloadError(@Cause Exception ex);

   @Message(value = "Invalid value %s for attribute %s: must be a number less than " + Integer.MAX_VALUE, id = 971)
   CacheConfigurationException attributeMustBeAnInteger(Number value, Enum<?> attribute);

   @LogMessage(level = INFO)
   @Message(value = "Task '%s', pending (%d), last check had (%d) pending, status is %s", id = 972)
   void taskProgression(String name, long pending, long lastCheck, String status);

   @LogMessage(level = INFO)
   @Message(value = "Task '%s' started at %s and done %s", id = 973)
   void taskDone(String name, Instant started, Instant completed);

   @Message(value = "Cannot instantiate class '%s'", id = 29523)
   CacheConfigurationException cannotInstantiateClass(String classname, @Suppressed Throwable t);
}
