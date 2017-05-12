package org.infinispan.scripting.impl;

import java.util.concurrent.CompletableFuture;

/**
 * ScriptRunner.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface ScriptRunner {
   <T> CompletableFuture<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptBindings binding);
}
