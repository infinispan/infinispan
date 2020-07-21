package org.infinispan.query.remote.impl.mapping;

import java.util.Arrays;
import java.util.Set;

import org.hibernate.search.mapper.pojo.mapping.definition.programmatic.ProgrammaticMappingConfigurationContext;
import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.impl.CacheRoutingKeyBridge;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.remote.impl.indexing.ProtobufEntityConverter;
import org.infinispan.query.remote.impl.mapping.model.ProtobufBootstrapIntrospector;
import org.infinispan.query.remote.impl.mapping.reference.GlobalReferenceHolder;
import org.infinispan.query.remote.impl.mapping.typebridge.ProtobufMessageBinder;
import org.infinispan.search.mapper.mapping.SearchMappingBuilder;
import org.infinispan.search.mapper.mapping.SearchMappingHolder;

public class SerializationContextSearchMapping {

   private static final String[] EXCLUDE_CACHE_NAMES = {"___script_cache", "___protobuf_metadata"};

   private final SerializationContext serializationContext;

   public static SerializationContextSearchMapping acquire(SerializationContext serializationContext) {
      return new SerializationContextSearchMapping(serializationContext);
   }

   private SerializationContextSearchMapping(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   public void updateSearchMappingHolders(EmbeddedCacheManager cacheManager) {
      for (String cacheName : cacheManager.getCacheConfigurationNames()) {
         if (Arrays.asList(EXCLUDE_CACHE_NAMES).contains(cacheName)) {
            continue;
         }

         // if the cache has not started yet, there is no reason to create SearchMapping,
         // it will be created at `org.infinispan.query.remote.impl.LifecycleManager#cacheStarting`,
         // that is the best way for Search 6.
         if (!cacheManager.isRunning(cacheName)) {
            continue;
         }

         Cache<Object, Object> cache = cacheManager.getCache(cacheName);
         SearchMappingHolder searchMappingHolder = ComponentRegistryUtils.getSearchMappingHolder(cache);
         if (searchMappingHolder != null) {
            buildMapping(searchMappingHolder, cache.getCacheConfiguration().indexing().indexedEntityTypes());
         }
      }
   }

   public void buildMapping(SearchMappingHolder mappingHolder, Set<String> indexedEntityTypes) {
      GlobalReferenceHolder globalReferenceHolder = new GlobalReferenceHolder(serializationContext.getGenericDescriptors());

      ProtobufBootstrapIntrospector introspector = new ProtobufBootstrapIntrospector();
      SearchMappingBuilder builder = mappingHolder.builder(introspector);
      builder.setEntityConverter(new ProtobufEntityConverter(serializationContext, globalReferenceHolder.getRootMessages()));
      ProgrammaticMappingConfigurationContext programmaticMapping = builder.programmaticMapping();

      if (globalReferenceHolder.getRootMessages().isEmpty()) {
         return;
      }

      boolean existIndexedEntities = false;
      for (GlobalReferenceHolder.RootMessageInfo rootMessage : globalReferenceHolder.getRootMessages()) {
         String fullName = rootMessage.getFullName();
         if (!indexedEntityTypes.contains(fullName)) {
            continue;
         }

         existIndexedEntities = true;

         programmaticMapping.type(fullName)
               .binder(new ProtobufMessageBinder(globalReferenceHolder, fullName))
               .routingKeyBinder(new CacheRoutingKeyBridge.Binder())
               .indexed().index(rootMessage.getIndexName());

         builder.addEntityType(byte[].class, fullName);
      }

      if (existIndexedEntities) {
         mappingHolder.build();
      }
   }
}
