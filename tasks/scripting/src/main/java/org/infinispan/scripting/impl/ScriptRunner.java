package org.infinispan.scripting.impl;

import java.util.concurrent.CompletionStage;

/**
 * ScriptRunner.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface ScriptRunner {
   <T> CompletionStage<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptBindings binding);
}
