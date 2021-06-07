package org.infinispan.scripting;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.tasks.TaskContext;

/**
 * ScriptingManager. Defines the operations that can be performed on scripts. Scripts are stored in
 * a dedicated cache.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface ScriptingManager {
   String SCRIPT_CACHE = "___script_cache";

   /**
    * @deprecated since 12.1. Will be removed in 15.0. Use the CREATE permission instead.
    */
   @Deprecated
   String SCRIPT_MANAGER_ROLE = "___script_manager";

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
    * Retrieves names of all available scripts.
    *
    * @return {@link Set<String>} containing names of available scripts.
    */
   Set<String> getScriptNames();
}
