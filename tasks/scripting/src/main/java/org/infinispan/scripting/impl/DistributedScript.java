package org.infinispan.scripting.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.scripting.ScriptingManager;

/**
 * DistributedScript.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
@SerializeWith(DistributedScript.Externalizer.class)
class DistributedScript<T> implements Function<EmbeddedCacheManager, T> {
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

   /**
    * Externalizer required for serialization when jboss-marshalling is not present. Eventually {@link DistributedScript}
    * will be marshalled via protostream annotations once the GlobalMarshaller has been converted and this class can be
    * removed.
    */
   public static class Externalizer implements org.infinispan.commons.marshall.Externalizer<DistributedScript> {
      @Override
      public void writeObject(ObjectOutput output, DistributedScript object) throws IOException {
         output.writeUTF(object.cacheName);
         output.writeObject(object.metadata);
         MarshallUtil.marshallMap(object.ctxParams, output);
      }

      @Override
      public DistributedScript readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String cacheName = input.readUTF();
         ScriptMetadata metadata = (ScriptMetadata) input.readObject();
         Map<String, ?> ctxParams = MarshallUtil.unmarshallMap(input, HashMap::new);
         return new DistributedScript<>(cacheName, metadata, ctxParams);
      }
   }
}
