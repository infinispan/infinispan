package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.util.Collection;
import java.util.Set;

import org.hibernate.search.engine.reporting.FailureHandler;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.infinispan.encoding.DataConversion;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.impl.EntityLoader;
import org.infinispan.query.remote.impl.logging.Log;
import org.infinispan.query.remote.impl.mapping.SerializationContextSearchMapping;
import org.infinispan.query.remote.impl.util.LazyRef;
import org.infinispan.search.mapper.mapping.SearchIndexedEntity;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.mapping.SearchMappingBuilder;
import org.infinispan.search.mapper.mapping.SearchMappingCommonBuilding;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.session.SearchSession;
import org.infinispan.search.mapper.work.SearchIndexer;

/**
 * @since 12.0
 */
public class LazySearchMapping implements SearchMapping {

   private static final Log log = LogFactory.getLog(LazySearchMapping.class, Log.class);

   private final Cache<?, ?> cache;
   private final ProtobufMetadataManagerImpl protobufMetadataManager;
   private final SearchMappingCommonBuilding commonBuilding;
   private final EntityLoader<?> entityLoader;
   private final SerializationContext serCtx;
   private final LazyRef<SearchMapping> searchMappingRef = new LazyRef<>(this::createMapping);

   public LazySearchMapping(SearchMappingCommonBuilding commonBuilding, EntityLoader<?> entityLoader,
                            SerializationContext serCtx, AdvancedCache<?, ?> cache,
                            ProtobufMetadataManagerImpl protobufMetadataManager) {
      this.commonBuilding = commonBuilding;
      this.entityLoader = entityLoader;
      this.serCtx = serCtx;
      this.cache = cache;
      this.protobufMetadataManager = protobufMetadataManager;
   }

   @Override
   public <E> SearchScope<E> scope(Collection<? extends Class<? extends E>> types) {
      return searchMappingRef.get().scope(types);
   }

   @Override
   public SearchScope<?> scopeAll() {
      return searchMappingRef.get().scopeAll();
   }

   @Override
   public FailureHandler getFailureHandler() {
      return searchMappingRef.get().getFailureHandler();
   }

   @Override
   public void close() {
      searchMappingRef.get().close();
   }

   @Override
   public boolean isClose() {
      return searchMappingRef.get().isClose();
   }

   @Override
   public SearchSession getMappingSession() {
      return searchMappingRef.get().getMappingSession();
   }

   @Override
   public SearchIndexer getSearchIndexer() {
      return searchMappingRef.get().getSearchIndexer();
   }

   @Override
   public SearchIndexedEntity indexedEntity(Class<?> entityType) {
      return searchMappingRef.get().indexedEntity(entityType);
   }

   @Override
   public SearchIndexedEntity indexedEntity(String entityName) {
      return searchMappingRef.get().indexedEntity(entityName);
   }

   @Override
   public Collection<? extends SearchIndexedEntity> allIndexedEntities() {
      return searchMappingRef.get().allIndexedEntities();
   }

   @Override
   public Set<String> allIndexedEntityNames() {
      return searchMappingRef.get().allIndexedEntityNames();
   }

   @Override
   public Set<Class<?>> allIndexedEntityJavaClasses() {
      return searchMappingRef.get().allIndexedEntityJavaClasses();
   }

   @Override
   public Class<?> toConvertedEntityJavaClass(Object value) {
      return searchMappingRef.get().toConvertedEntityJavaClass(value);
   }

   private SearchMapping createMapping() {
      IndexingConfiguration indexingConfiguration = cache.getCacheConfiguration().indexing();
      Set<String> indexedEntityTypes = indexingConfiguration.indexedEntityTypes();
      DataConversion valueDataConversion = cache.getAdvancedCache().getValueDataConversion();

      SearchMapping searchMapping = null;
      if (commonBuilding != null) {
         SearchMappingBuilder builder = SerializationContextSearchMapping.createBuilder(commonBuilding, entityLoader, indexedEntityTypes, serCtx);
         searchMapping = builder != null ? builder.build() : null;
      }
      if (indexingConfiguration.enabled()) {
         if (valueDataConversion.getStorageMediaType().match(APPLICATION_PROTOSTREAM)) {
            // Try to resolve the indexed type names to protobuf type names.
            Set<String> knownTypes = protobufMetadataManager.getSerializationContext().getGenericDescriptors().keySet();
            for (String typeName : indexedEntityTypes) {
               if (!knownTypes.contains(typeName)) {
                  if (searchMapping != null) searchMapping.close();
                  throw log.unknownType(typeName);
               }
               if (searchMapping == null || searchMapping.indexedEntity(typeName) == null) {
                  if (searchMapping != null) searchMapping.close();
                  throw log.typeNotIndexed(typeName);
               }
            }
         }
      }
      return searchMapping;
   }
}
