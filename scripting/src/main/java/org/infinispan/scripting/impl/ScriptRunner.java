package org.infinispan.scripting.impl;

import org.infinispan.commons.util.concurrent.NotifyingFuture;

/**
 * ScriptRunner.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public interface ScriptRunner {
   <T> NotifyingFuture<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptBindings binding);
}
