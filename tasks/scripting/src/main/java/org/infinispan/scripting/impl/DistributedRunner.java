package org.infinispan.scripting.impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.manager.ClusterExecutor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.scripting.logging.Log;
import org.infinispan.util.function.TriConsumer;
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
      Map<String, Object> ctxParams = extractContextParams(metadata, binding);
      ClusterExecutor clusterExecutor = masterCacheNode.getCacheManager().executor();
      List<T> results = new ArrayList<>();
      TriConsumer<Address, T, Throwable> triConsumer = (a, v, t) -> {
         if (t != null) {
            throw new CacheException(t);
         }
         synchronized (this) {
            results.add(v);
         }
      };
      CompletableFuture<Void> future = clusterExecutor.submitConsumer(new DistributedScript<>(masterCacheNode.getName(), metadata, ctxParams), triConsumer);

      return (CompletableFuture<T>) future.thenApply(ignore -> results);
   }

   private Map<String, Object> extractContextParams(ScriptMetadata metadata, CacheScriptBindings binding) {
      Map<String, Object> params = new HashMap<>();
      metadata.parameters().forEach(paramName -> params.put(paramName, binding.get(paramName)));
      return params;
   }

}
