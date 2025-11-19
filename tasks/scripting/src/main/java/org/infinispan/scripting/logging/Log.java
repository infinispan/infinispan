package org.infinispan.scripting.logging;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.CacheException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log abstraction for the Scripting module.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 27501, max = 28000)
public interface Log extends BasicLogger {
   static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(MethodHandles.lookup(), Log.class, clazz.getName());
   }

//   @LogMessage(level = ERROR)
//   @Message(value = "Could not register interpreter MBean", id = 27501)
//   void jmxRegistrationFailed();

//   @LogMessage(level = ERROR)
//   @Message(value = "Could not unregister interpreter MBean", id = 27502)
//   void jmxUnregistrationFailed();

   @Message(value = "Script execution error", id = 27503)
   CacheException scriptExecutionError(@Cause Throwable t);

   @Message(value = "Compiler error for script '%s'", id = 27504)
   CacheException scriptCompilationException(@Cause Throwable t, String name);

   @Message(value = "No script named '%s'", id = 27505)
   CacheException noNamedScript(String name);

   @Message(value = "Unknown script mode: '%s'", id = 27506)
   CacheException unknownScriptProperty(String value);

   @Message(value = "Cannot find an appropriate script engine for script '%s'", id = 27507)
   IllegalArgumentException noScriptEngineForScript(String name);

   @Message(value = "Script '%s' cannot be invoked directly since it specifies mode '%s'", id = 27508)
   IllegalArgumentException cannotInvokeScriptDirectly(String scriptName, String property);

   @Message(value = "Distributed script '%s' invoked without a cache binding", id = 27509)
   IllegalStateException distributedTaskNeedCacheInBinding(String scriptName);

   @Message(value = "Script parameters must be declared using the unquoted array notation, e.g. [a,b,c]", id = 27511)
   IllegalArgumentException parametersNotArray();

   @Message(value = "Scripts can only access named caches", id = 27512)
   IllegalArgumentException scriptsCanOnlyAccessNamedCaches();

   @Message(value = "Script properties must be declared using the unquoted object notation, e.g. {name: value}", id = 27513)
   IllegalArgumentException propertiesNotObject();

}
