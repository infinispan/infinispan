package org.infinispan.scripting;

import static org.infinispan.commons.internal.InternalCacheNames.SCRIPT_CACHE_NAME;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.scripting.impl.ScriptMetadata;
import org.infinispan.scripting.impl.ScriptWithMetadata;
import org.infinispan.tasks.TaskContext;

/**
 * ScriptingManager. Defines the operations that can be performed on scripts. Scripts are stored in
 * a dedicated cache.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface ScriptingManager {
   @Deprecated(forRemoval = true, since = "15.0")
   String SCRIPT_CACHE = SCRIPT_CACHE_NAME;

   /**
    * Adds a new named script.
    *
    * @param name
    *           the name of the script. The name should contain an extension identifying its
    *           language
    * @param script
    *           the source of the script
    */
   void addScript(String name, String script);

   /**
    * Adds a new named script with user-specified metadata
    *
    * @param name
    *           the name of the script. The name should contain an extension identifying its
    *           language
    * @param metadata the metadata for the script
    * @param script
    *           the source of the script
    */
   void addScript(String name, String script, ScriptMetadata metadata);

   /**
    * Removes a script.
    *
    * @param name
    *           the name of the script ro remove
    */
   void removeScript(String name);

   /**
    * Runs a named script
    *
    * @param scriptName The name of the script to run. Use {@link #addScript(String, String)} to add a script
    * @return a {@link CompletableFuture} which will return the result of the script execution
    */
   <T> CompletionStage<T> runScript(String scriptName);

   /**
    * Runs a named script using the specified {@link TaskContext}
    *
    * @param scriptName The name of the script to run. Use {@link #addScript(String, String)} to add a script
    * @param context A {@link TaskContext} within which the script will be executed
    * @return a {@link CompletableFuture} which will return the result of the script execution
    */
   <T> CompletionStage<T> runScript(String scriptName, TaskContext context);

   /**
    * Retrieves the source code of an existing script.
    *
    * @param scriptName The name of the script
    * @return the source code of the script
     */
   String getScript(String scriptName);

   /**
    * Retrieves the source code of an existing script together with its metadata
    *
    * @param scriptName The name of the script
    * @return the source code of the script
    */
   ScriptWithMetadata getScriptWithMetadata(String scriptName);

   /**
    * Retrieves names of all available scripts.
    *
    * @return {@link Set<String>} containing names of available scripts.
    */
   Set<String> getScriptNames();

   /**
    * Returns whether a script exists
    * @param scriptName the name of the script
    * @return a boolean indicating script existence
    */
   boolean containsScript(String scriptName);
}
