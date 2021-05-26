package org.infinispan.scripting.impl;

import java.util.concurrent.CompletionStage;

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
   public <T> CompletionStage<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptBindings bindings) {
      return scriptManager.execute(metadata, bindings);
   }

}
