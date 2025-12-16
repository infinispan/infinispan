package org.infinispan.scripting.impl;

import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.dataconversion.MediaType;
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

   // TODO: this smells bugs - remove it possibly in a subsequent iteration
   ObjectMapper objectMapper = new ObjectMapper();

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

      MediaType scriptMediaType = metadata.dataType();
      DataTypedCacheManager dataTypedCacheManager = new DataTypedCacheManager(scriptMediaType, embeddedCacheManager, null);
      Cache cache = embeddedCacheManager.getCache(cacheName).getAdvancedCache();

      // verify application of user local bindings
      // TODO populate systemBindings ? is it even used? consider removing
      JsonNode systemBindings = JsonNodeFactory.instance.objectNode();

      // no user bindings? how to propagate those? or is it not necessary?
      ObjectNode userBindings = JsonNodeFactory.instance.objectNode();
      ctxParams.entrySet().forEach(param ->
              userBindings.put(param.getKey(), objectMapper.valueToTree(param.getValue())));

      CacheScriptArguments args = new CacheScriptArguments(
              systemBindings,
              userBindings,
              cache,
              dataTypedCacheManager);

      try {
         return CompletionStages.join(scriptManager.execute(metadata, args));
      } catch (CompletionException e) {
         throw new CacheException(e.getCause());
      }
   }
}
