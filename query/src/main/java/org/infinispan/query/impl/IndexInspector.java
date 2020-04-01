package org.infinispan.query.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.hibernate.search.indexes.spi.IndexManager;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
import org.hibernate.search.spi.impl.PojoIndexedTypeIdentifier;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * Provides runtime information about indexing backends.
 *
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public final class IndexInspector {
   private static final String ELASTICSEARCH_INDEX_MANAGER = "org.hibernate.search.elasticsearch.impl.ElasticsearchIndexManager";
   private final Map<Class<?>, IndexManager> indexManagerPerClass = new HashMap<>(2);
   private final Map<String, Class<?>> indexedEntities;
   private final SearchIntegrator searchFactory;
   private boolean hasLocalIndexes;

   public IndexInspector(Configuration cfg, SearchIntegrator searchFactory) {
      Map<String, Class<?>> entities = new HashMap<>(2);
      for (Class<?> c : cfg.indexing().indexedEntities()) {
         // include classes declared in indexing config
         entities.put(c.getName(), c);
      }
      for (IndexedTypeIdentifier typeIdentifier : searchFactory.getIndexBindings().keySet()) {
         // include possible programmatically declared classes via SearchMapping
         Class<?> c = typeIdentifier.getPojoType();
         entities.put(c.getName(), c);
         hasLocalIndexes |= searchFactory.getIndexBinding(typeIdentifier).getIndexManagerSelector().all()
               .stream().anyMatch(i -> !isShared(i));
      }
      indexedEntities = Collections.unmodifiableMap(entities);
      this.searchFactory = searchFactory;
   }

   // TODO: This is being done lazily as sometimes the Infinispan Directory is not initialized at search factory creation.
   Map<Class<?>, IndexManager> getIndexManagerPerClass() {
      if (indexManagerPerClass.isEmpty()) {
         for (Class<?> c : indexedEntities.values()) {
            PojoIndexedTypeIdentifier typeIdentifier = new PojoIndexedTypeIdentifier(c);
            Set<IndexManager> indexManagers = searchFactory.getIndexBinding(typeIdentifier).getIndexManagerSelector().all();
            Set<? extends Class<? extends IndexManager>> ims = indexManagers.stream().map(IndexManager::getClass).collect(Collectors.toSet());
            if (ims.size() > 1) {
               throw new IllegalStateException("Different Shards using different index managers are not supported");
            }
            indexManagerPerClass.put(c, indexManagers.iterator().next());
         }
      }
      return indexManagerPerClass;
   }

   public boolean hasSharedIndex(Class<?> entity) {
      IndexManager indexManager = getIndexManagerPerClass().get(entity);
      if (indexManager == null) return false;
      return isShared(indexManager);
   }

   private boolean isShared(IndexManager indexManager) {
      return indexManager.getClass().getName().equals(ELASTICSEARCH_INDEX_MANAGER);
   }

   public boolean hasLocalIndexes() {
      return hasLocalIndexes;
   }

   public Map<String, Class<?>> getIndexedEntities() {
      return indexedEntities;
   }

   public boolean isIndexedType(Object value) {
      if (value == null) {
         return false;
      }
      Class<?> c = value.getClass();
      return indexedEntities.containsValue(c);
   }
}
