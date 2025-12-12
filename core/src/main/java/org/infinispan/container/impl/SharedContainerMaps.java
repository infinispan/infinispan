package org.infinispan.container.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

@Scope(Scopes.GLOBAL)
public class SharedContainerMaps {
   private final Map<String, SharedCaffeineMap<Object, Object>> maps = new ConcurrentHashMap<>();

   @Inject GlobalConfiguration globalConfiguration;

   public Map<String, SharedCaffeineMap<Object, Object>> getMaps() {
      return maps;
   }

   @Start
   public void start() {
      globalConfiguration.getMemoryContainer().forEach((name, config) -> {
         boolean sizeInBytes = config.maxSize() != null;
         long thresholdSize = sizeInBytes ? config.maxSizeBytes() : config.maxCount();
         maps.put(name, new SharedCaffeineMap<>(thresholdSize, sizeInBytes));
      });
   }

   @Stop
   public void stop() {
      maps.clear();
   }

   public <K, V> SharedCaffeineMap<K, V> getMap(String containerName) {
      SharedCaffeineMap<K, V> map = (SharedCaffeineMap<K, V>) maps.get(containerName);
      if (map == null) {
         throw new IllegalStateException("There is no container of name: " + containerName);
      }
      return map;
   }
}
