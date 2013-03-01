package org.infinispan.scripting.impl;

import org.infinispan.commons.util.concurrent.NotifyingFuture;

/**
 * LocalRunner.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class LocalRunner implements ScriptRunner {
   public static final LocalRunner INSTANCE = new LocalRunner();

   private LocalRunner() {
   }

   @Override
   public <T> NotifyingFuture<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptBindings bindings) {
      return scriptManager.execute(metadata, bindings);
   }

}
