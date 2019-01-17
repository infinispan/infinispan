package org.infinispan.scripting.impl;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.ScriptingManager;

/**
 * DistributedScript.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
class DistributedScript<T> implements Function<EmbeddedCacheManager, T>, Serializable {
   private final String cacheName;
   private final ScriptMetadata metadata;
   private final Map<String, ?> ctxParams;

   DistributedScript(String cacheName, ScriptMetadata metadata, Map<String, ?> ctxParams) {
      this.cacheName = cacheName;
      this.metadata = metadata;
      this.ctxParams = ctxParams;
   }

   @Override
   public T apply(EmbeddedCacheManager embeddedCacheManager) {
      ScriptingManagerImpl scriptManager = (ScriptingManagerImpl) SecurityActions.getGlobalComponentRegistry(embeddedCacheManager).getComponent(ScriptingManager.class);
      Bindings bindings = new SimpleBindings();

      String scriptMediaType = metadata.dataType().toString();
      DataTypedCacheManager dataTypedCacheManager = new DataTypedCacheManager(scriptMediaType, embeddedCacheManager, null);
      bindings.put("cacheManager", dataTypedCacheManager);
      AdvancedCache<?, ?> cache = embeddedCacheManager.getCache(cacheName).getAdvancedCache();
      bindings.put("cache", cache.withMediaType(scriptMediaType, scriptMediaType));
      ctxParams.forEach((key, value) -> bindings.put(key, value));

      try {
         return (T) (scriptManager.execute(metadata, bindings).get());
      } catch (InterruptedException | ExecutionException e) {
         throw new CacheException(e);
      }
   }
}
