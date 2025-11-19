package org.infinispan.scripting.impl;

import java.util.concurrent.CompletableFuture;

import org.infinispan.scripting.logging.Log;

/**
 * NullRunner.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class NullRunner implements ScriptRunner {
   static final Log log = Log.getLog(NullRunner.class);
   public static final NullRunner INSTANCE = new NullRunner();

   private NullRunner() {
   }

   @Override
   public <T> CompletableFuture<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptBindings binding) {
      throw log.cannotInvokeScriptDirectly(metadata.name(), metadata.mode().toString());
   }

}
