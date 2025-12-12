package org.infinispan.scripting.impl;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.JsonNode;
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
   public <T> CompletableFuture<T> runScript(ScriptingManagerImpl scriptManager, ScriptMetadata metadata, CacheScriptArguments args) {
      Cache<?, ?> masterCacheNode = args.getCache();
      if (masterCacheNode == null || masterCacheNode.getCacheManager() == null) {
         throw log.distributedTaskNeedCacheInBinding(metadata.name());
      }
      Map<String, Object> ctxParams = extractContextParams(metadata, args);
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

   private Map<String, Object> extractContextParams(ScriptMetadata metadata, CacheScriptArguments arguments) {
      Map<String, Object> params = new HashMap<>();
      JsonNode json = arguments.getUserInput();
      json.fieldNames().forEachRemaining(paramName -> params.put(paramName, toJava(json.get(paramName))));
      return params;
   }

   public static Object toJava(JsonNode node) {
      if (node.isObject()) {
         Map<String, Object> map = new LinkedHashMap<>();
         node.fieldNames().forEachRemaining(paramName -> map.put(paramName, toJava(node.get(paramName))));
         return map;
      }
      if (node.isArray()) {
         List<Object> list = new ArrayList<>();
         node.forEach(item -> list.add(toJava(item)));
         return list;
      }
      if (node.isNumber()) return node.numberValue();
      if (node.isBoolean()) return node.booleanValue();
      if (node.isNull()) return null;
      return node.asText();
   }

}
