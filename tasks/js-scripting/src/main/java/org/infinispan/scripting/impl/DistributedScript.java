package org.infinispan.scripting.impl;

import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.protostream.impl.MarshallableMap;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.scripting.ScriptingManager;
import org.infinispan.security.actions.SecurityActions;

/**
 * DistributedScript.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@ProtoTypeId(ProtoStreamTypeIds.DISTRIBUTED_SCRIPT)
class DistributedScript<T> implements Function<EmbeddedCacheManager, T> {

   @ProtoField(1)
   final String cacheName;

   @ProtoField(2)
   final ScriptMetadata metadata;

   private final Map<String, ?> ctxParams;

   DistributedScript(String cacheName, ScriptMetadata metadata, Map<String, ?> ctxParams) {
      this.cacheName = cacheName;
      this.metadata = metadata;
      this.ctxParams = ctxParams;
   }

   @ProtoFactory
   DistributedScript(String cacheName, ScriptMetadata metadata, MarshallableMap<String, ?> ctxParams) {
      this(cacheName, metadata, MarshallableMap.unwrap(ctxParams));
   }

   @ProtoField(3)
   MarshallableMap<String, ?> getCtxParams() {
      return MarshallableMap.create(ctxParams);
   }

   @Override
   public T apply(EmbeddedCacheManager embeddedCacheManager) {
      ScriptingManagerImpl scriptManager = (ScriptingManagerImpl) SecurityActions.getGlobalComponentRegistry(embeddedCacheManager).getComponent(ScriptingManager.class);

// TODO: this should go in Builtins
// verify the functionality!
//      Bindings bindings = new SimpleBindings();
//
//      MediaType scriptMediaType = metadata.dataType();
//      DataTypedCacheManager dataTypedCacheManager = new DataTypedCacheManager(scriptMediaType, embeddedCacheManager, null);
//      bindings.put("cacheManager", dataTypedCacheManager);
//      AdvancedCache<?, ?> cache = embeddedCacheManager.getCache(cacheName).getAdvancedCache();
//      bindings.put("cache", cache.withMediaType(scriptMediaType, scriptMediaType));
//      ctxParams.forEach(bindings::put);

      // verify application of user local bindings

      try {
         // TODO: FIXME the null
         return CompletionStages.join(scriptManager.execute(metadata, null));
      } catch (CompletionException e) {
         throw new CacheException(e.getCause());
      }
   }
}
