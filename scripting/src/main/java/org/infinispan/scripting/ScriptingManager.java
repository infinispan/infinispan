package org.infinispan.scripting;

import javax.script.Bindings;
import javax.script.ScriptEngine;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.util.concurrent.NotifyingFuture;

/**
 * ScriptingManager. Defines the operations that can be performed on scripts. Scripts are stored in
 * a dedicated cache.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface ScriptingManager {

   /**
    * Adds a new named script.
    *
    * @param name
    *           the name of the script. The name should contain an extension identifying its
    *           language
    * @param script
    *           the source of the script
    * @param mimeType
    *           the mimeType of the source which selects the {@link ScriptEngine} to use to compile
    *           the script
    */
   void addScript(String name, String script);

   /**
    * Removes a script.
    *
    * @param name
    *           the name of the script ro remove
    */
   void removeScript(String name);

   /**
    * Configures a custom marshaller to be exposed to scripts
    * as a binding variable
    *
    * @param marshaller
    */
   void setMarshaller(Marshaller marshaller);

   /**
    * @return configured marshaller
    */
   Marshaller getMarshaller();

   /**
    * Runs a named script
    *
    * @param scriptName The name of the script to run. Use {@link #addScript(String, String)} to add a script
    * @return a {@link NotifyingFuture} which will return the result of the script execution
    */
   <T> NotifyingFuture<T> runScript(String scriptName);

   /**
    * Runs a named script using the specified user bindings
    *
    * @param scriptName The name of the script to run. Use {@link #addScript(String, String)} to add a script
    * @param parameters The user parameters that will be combined with the system bindings and made available to the script
    * @return a {@link NotifyingFuture} which will return the result of the script execution
    */
   <T> NotifyingFuture<T> runScript(String scriptName, Bindings parameters);

   /**
    * Runs a named script using the specified cache as a "driver". Use this form when invoking clustered scripts (i.e. distributed, map/reduce)
    *
    * @param scriptName The name of the script to run. Use {@link #addScript(String, String)} to add a script
    * @param cache A cache to add to the system bindings. The parameter is compulsory if running clustered scripts
    * @return a {@link NotifyingFuture} which will return the result of the script execution
    */
   <T> NotifyingFuture<T> runScript(String scriptName, Cache<?, ?> cache);

   /**
    * Runs a named script using the specified user bindings and the specified cache as a "driver". Use this form when invoking clustered scripts (i.e. distributed, map/reduce)
    *
    * @param scriptName The name of the script to run. Use {@link #addScript(String, String)} to add a script
    * @param cache A cache to add to the system bindings. The parameter is compulsory if running clustered scripts
    * @param parameters The user parameters that will be combined with the system bindings and made available to the script
    * @return a {@link NotifyingFuture} which will return the result of the script execution
    */
   <T> NotifyingFuture<T> runScript(String scriptName, Cache<?, ?> cache, Bindings parameters);

}
