package org.infinispan.scripting.impl;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.scripting.logging.Log;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.LogFactory;

/**
 * DistributedRunner.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class DistributedRunner implements ScriptRunner {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);
   public static final DistributedRunner INSTANCE = new DistributedRunner();

   private DistributedRunner() {
   }

   @Override
   public <T> CompletableFuture<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptBindings binding) {
      Cache<?, ?> masterCacheNode = (Cache<?, ?>) binding.get(SystemBindings.CACHE.toString());
      if (masterCacheNode == null) {
         throw log.distributedTaskNeedCacheInBinding(metadata.name());
      }
      DefaultExecutorService des = new DefaultExecutorService(masterCacheNode);
      try {
         Map<String, Object> ctxParams = extractContextParams(metadata, binding);
         List<CompletableFuture<T>> tasks = des.submitEverywhere(new DistributedScript<T>(metadata, ctxParams));

         return (CompletableFuture<T>) CompletableFutures.sequence(tasks);
      } finally {
         des.shutdown();
      }
   }

   private Map<String, Object> extractContextParams(ScriptMetadata metadata, CacheScriptBindings binding) {
      Map<String, Object> params = new HashMap<>();
      metadata.parameters().stream().forEach(paramName -> params.put(paramName, binding.get(paramName)));
      return params;
   }

}
