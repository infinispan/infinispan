package org.infinispan.scripting.impl;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.UTF8Encoder;
import org.infinispan.distexec.DistributedCallable;
import org.infinispan.scripting.ScriptingManager;

/**
 * DistributedScript.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
class DistributedScript<T> implements DistributedCallable<Object, Object, T>, Serializable {
   private final ScriptMetadata metadata;
   private final Map<String, ?> ctxParams;
   private transient ScriptingManagerImpl scriptManager;
   private transient Bindings bindings;

   DistributedScript(ScriptMetadata metadata, Map<String, ?> ctxParams) {
      this.metadata = metadata;
      this.ctxParams = ctxParams;
   }

   @Override
   public T call() throws Exception {
      return (T) (scriptManager.execute(metadata, bindings).get());
   }

   @Override
   public void setEnvironment(Cache<Object, Object> cache, Set<Object> inputKeys) {
      scriptManager = (ScriptingManagerImpl) SecurityActions.getGlobalComponentRegistry(cache.getCacheManager()).getComponent(ScriptingManager.class);
      bindings = new SimpleBindings();
      bindings.put("inputKeys", inputKeys);
      DataType dataType = metadata.dataType();
      DataTypedCacheManager dataTypedCacheManager = new DataTypedCacheManager(dataType, Optional.empty(), cache.getCacheManager(), null);
      bindings.put("cacheManager", dataTypedCacheManager);
      if (dataType == DataType.UTF8) {
         cache = (Cache<Object, Object>) cache.getAdvancedCache().withEncoding(UTF8Encoder.class);
      } else {
         cache = cache.getAdvancedCache();
      }
      bindings.put("cache", cache);
      ctxParams.entrySet().stream().forEach(e -> bindings.put(e.getKey(), e.getValue()));
   }
}
