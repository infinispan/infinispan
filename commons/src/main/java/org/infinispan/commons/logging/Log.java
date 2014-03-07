package org.infinispan.commons.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

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

}

