package org.infinispan.query.remote.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;

import org.hibernate.search.engine.common.spi.SearchIntegration;
import org.hibernate.search.engine.reporting.FailureHandler;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.configuration.cache.IndexingConfiguration;
import org.infinispan.encoding.DataConversion;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.query.core.impl.QueryCache;
import org.infinispan.query.impl.EntityLoader;
import org.infinispan.query.remote.impl.logging.Log;
import org.infinispan.query.remote.impl.mapping.SerializationContextSearchMapping;
import org.infinispan.query.remote.impl.util.LazyRef;
import org.infinispan.search.mapper.mapping.SearchIndexedEntity;
import org.infinispan.search.mapper.mapping.SearchMapping;
import org.infinispan.search.mapper.mapping.SearchMappingBuilder;
import org.infinispan.search.mapper.mapping.SearchMappingCommonBuilding;
import org.infinispan.search.mapper.mapping.metamodel.IndexMetamodel;
import org.infinispan.search.mapper.mapping.impl.InfinispanMapping;
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
   private final QueryCache queryCache;

   private LazyRef<SearchMapping> searchMappingRef = new LazyRef<>(this::createMapping);
   private final StampedLock stampedLock = new StampedLock();

   private volatile boolean restarting = false;

   public LazySearchMapping(SearchMappingCommonBuilding commonBuilding, EntityLoader<?> entityLoader,
                            SerializationContext serCtx, AdvancedCache<?, ?> cache,
                            ProtobufMetadataManagerImpl protobufMetadataManager, QueryCache queryCache) {
      this.commonBuilding = commonBuilding;
      this.entityLoader = entityLoader;
      this.serCtx = serCtx;
      this.cache = cache;
      this.protobufMetadataManager = protobufMetadataManager;
      this.queryCache = queryCache;
   }

   @Override
   public <E> SearchScope<E> scope(Collection<? extends Class<? extends E>> types) {
      return mapping().scope(types);
   }

   @Override
   public SearchScope<?> scopeAll() {
      return mapping().scopeAll();
   }

   @Override
   public FailureHandler getFailureHandler() {
      return mapping().getFailureHandler();
   }

   @Override
   public void close() {
      mapping().close();
   }

   @Override
   public boolean isClose() {
      return mapping().isClose();
   }

   @Override
   public boolean isRestarting() {
      return restarting;
   }

   @Override
   public SearchSession getMappingSession() {
      return mapping().getMappingSession();
   }

   @Override
   public SearchIndexer getSearchIndexer() {
      return mapping().getSearchIndexer();
   }

   @Override
   public SearchIndexedEntity indexedEntity(Class<?> entityType) {
      return mapping().indexedEntity(entityType);
   }

   @Override
   public SearchIndexedEntity indexedEntity(String entityName) {
      return mapping().indexedEntity(entityName);
   }

   @Override
   public Collection<? extends SearchIndexedEntity> allIndexedEntities() {
      return mapping().allIndexedEntities();
   }

   @Override
   public Collection<? extends SearchIndexedEntity> indexedEntitiesForStatistics() {
      long stamp = stampedLock.tryReadLock();
      if (stamp == 0L) {
         return Collections.emptySet();
      }

      try {
         if (!searchMappingRef.available()) {
            return Collections.emptySet();
         }
         return allIndexedEntities();
      } finally {
         stampedLock.unlockRead(stamp);
      }
   }

   @Override
   public Set<String> allIndexedEntityNames() {
      return mapping().allIndexedEntityNames();
   }

   @Override
   public Set<Class<?>> allIndexedEntityJavaClasses() {
      // Create the mapping is a very expensive and blocking operation.
      // It is better to avoid to invoke it if we don't need it.
      long stamp = stampedLock.tryReadLock();
      if (stamp == 0L) {
         return Collections.singleton(byte[].class);
      }

      try {
         if (!searchMappingRef.available()) {
            return Collections.singleton(byte[].class);
         }
         return mapping().allIndexedEntityJavaClasses();
      } finally {
         stampedLock.unlockRead(stamp);
      }
   }

   @Override
   public Class<?> toConvertedEntityJavaClass(Object value) {
      return mapping().toConvertedEntityJavaClass(value);
   }

   @Override
   public Map<String, IndexMetamodel> metamodel() {
      return mapping().metamodel();
   }

   @Override
   public int genericIndexingFailures() {
      long stamp = stampedLock.tryReadLock();
      if (stamp == 0L) {
         return -1;
      }

      try {
         if (!searchMappingRef.available()) {
            return -1;
         }
         return mapping().genericIndexingFailures();
      } finally {
         stampedLock.unlockRead(stamp);
      }
   }

   @Override
   public int entityIndexingFailures() {
      long stamp = stampedLock.tryReadLock();
      if (stamp == 0L) {
         return -1;
      }

      try {
         if (!searchMappingRef.available()) {
            return -1;
         }
         return mapping().entityIndexingFailures();
      } finally {
         stampedLock.unlockRead(stamp);
      }
   }

   @Override
   public void reload() {
      long stamp = stampedLock.writeLock();
      queryCache.clear(cache.getName());
      try {
         searchMappingRef.get().close();
         searchMappingRef = new LazyRef<>(this::createMapping);
      } finally {
         stampedLock.unlockWrite(stamp);
      }
   }

   @Override
   public void restart() {
      long stamp = stampedLock.writeLock();
      try {
         restarting = true;
         InfinispanMapping mapping = (InfinispanMapping) searchMappingRef.get();
         searchMappingRef = new LazyRef<>(() -> createMapping(Optional.of(mapping.getIntegration())));
         searchMappingRef.get(); // create it now
      } finally {
         restarting = false;
         stampedLock.unlockWrite(stamp);
      }
   }

   private SearchMapping mapping() {
      long stamp = stampedLock.tryOptimisticRead();
      SearchMapping searchMapping = searchMappingRef.get();
      if (!stampedLock.validate(stamp)) {
         stamp = stampedLock.readLock();
         try {
            searchMapping = searchMappingRef.get();
         } finally {
            stampedLock.unlockRead(stamp);
         }
      }
      return searchMapping;
   }

   private SearchMapping createMapping() {
      return createMapping(Optional.empty());
   }

   private SearchMapping createMapping(Optional<SearchIntegration> previousIntegration) {
      IndexingConfiguration indexingConfiguration = cache.getCacheConfiguration().indexing();
      Set<String> indexedEntityTypes = indexingConfiguration.indexedEntityTypes();
      DataConversion valueDataConversion = cache.getAdvancedCache().getValueDataConversion();

      SearchMapping searchMapping = null;
      if (commonBuilding != null) {
         SearchMappingBuilder builder = SerializationContextSearchMapping.createBuilder(commonBuilding, entityLoader, indexedEntityTypes, serCtx);
         searchMapping = builder != null ? builder.build(previousIntegration) : null;
      }
      if (indexingConfiguration.enabled()) {
         if (valueDataConversion.getStorageMediaType().match(APPLICATION_PROTOSTREAM)) {
            // Try to resolve the indexed type names to protobuf type names.
            Set<String> knownTypes = protobufMetadataManager.getKnownTypes();
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
