package org.infinispan.scripting.logging;

import static org.jboss.logging.Logger.Level.ERROR;

import org.infinispan.commons.CacheException;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

/**
 * Log abstraction for the Scripting module. For this module, message ids ranging from 21001 to
 * 22000 inclusively have been reserved.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@MessageLogger(projectCode = "ISPN")
public interface Log extends org.infinispan.util.logging.Log {
   @LogMessage(level = ERROR)
   @Message(value = "Could not register interpreter MBean", id = 21001)
   void jmxRegistrationFailed();

   @LogMessage(level = ERROR)
   @Message(value = "Could not unregister interpreter MBean", id = 21002)
   void jmxUnregistrationFailed();

   @Message(value = "Script execution error", id = 21003)
   CacheException scriptExecutionError(@Cause Throwable t);

   @Message(value = "Compiler error for script '%s'", id = 21004)
   CacheException scriptCompilationException(@Cause Throwable t, String name);

   @Message(value = "No script named '%s'", id = 21005)
   CacheException noNamedScript(String name);

   @Message(value = "Unknown script mode: '%s'", id = 21006)
   CacheException unknownScriptProperty(String value);

   @Message(value = "Cannot find an appropriate script engine for '%s'", id = 21007)
   IllegalArgumentException noScriptEngineForScript(String name);

   @Message(value = "Script '%s' cannot be invoked directly since it specifies mode '%s'", id = 21008)
   IllegalArgumentException cannotInvokeScriptDirectly(String scriptName, String property);

   @Message(value = "Distributed script '%s' invoked without a cache binding", id = 21009)
   IllegalStateException distributedTaskNeedCacheInBinding(String scriptName);

   @Message(value = "Cannot find an appropriate script engine for script '%s'", id = 21010)
   IllegalArgumentException noEngineForScript(String name);
}
