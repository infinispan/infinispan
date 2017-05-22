package org.infinispan.commons.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

import java.io.IOException;

import org.infinispan.commons.CacheConfigurationException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Infinispan's log abstraction layer on top of JBoss Logging.
 * <p/>
 * It contains explicit methods for all INFO or above levels so that they can
 * be internationalized. For the commons module, message ids ranging from 0901
 * to 1000 inclusively have been reserved.
 * <p/>
 * <code> Log log = LogFactory.getLog( getClass() ); </code> The above will get
 * you an instance of <tt>Log</tt>, which can be used to generate log messages
 * either via JBoss Logging which then can delegate to Log4J (if the libraries
 * are present) or (if not) the built-in JDK logger.
 * <p/>
 * In addition to the 6 log levels available, this framework also supports
 * parameter interpolation, similar to the JDKs {@link String#format(String, Object...)}
 * method. What this means is, that the following block:
 * <code> if (log.isTraceEnabled()) { log.trace("This is a message " + message + " and some other value is " + value); }
 * </code>
 * <p/>
 * ... could be replaced with ...
 * <p/>
 * <code> if (log.isTraceEnabled()) log.tracef("This is a message %s and some other value is %s", message, value);
 * </code>
 * <p/>
 * This greatly enhances code readability.
 * <p/>
 * If you are passing a <tt>Throwable</tt>, note that this should be passed in
 * <i>before</i> the vararg parameter list.
 * <p/>
 *
 * @author Manik Surtani
 * @since 4.0
 * @private
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   @LogMessage(level = WARN)
   @Message(value = "Property %s could not be replaced as intended!", id = 901)
   void propertyCouldNotBeReplaced(String line);

   @LogMessage(level = WARN)
   @Message(value = "Invocation of %s threw an exception %s. Exception is ignored.", id = 902)
   void ignoringException(String methodName, String exceptionName, @Cause Throwable t);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to set value!", id = 903)
   void unableToSetValue(@Cause Exception e);

   @Message(value = "Error while initializing SSL context", id = 904)
   CacheConfigurationException sslInitializationException(@Cause Throwable e);

   @LogMessage(level = ERROR)
   @Message(value = "Unable to load %s from any of the following classloaders: %s", id=905)
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

   @Message(value = "Expecting a unprotected configuration for %s", id = 913)
   IllegalStateException protectedAttributeSet(String name);

   @Message(value = "Duplicate attribute '%s' in attribute set '%s'", id = 914)
   IllegalArgumentException attributeSetDuplicateAttribute(String name, String setName);

   @Message(value = "No such attribute '%s' in attribute set '%s'", id = 915)
   IllegalArgumentException noSuchAttribute(String name, String setName);

   @Message(value = "No attribute copier for type '%s'", id = 916)
   IllegalArgumentException noAttributeCopierForType(Class<?> klass);

   @Message(value = "Cannot resize unbounded container", id = 917)
   UnsupportedOperationException cannotResizeUnboundedContainer();

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
}
