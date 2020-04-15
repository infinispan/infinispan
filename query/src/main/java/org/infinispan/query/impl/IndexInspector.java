package org.infinispan.query.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.hibernate.search.spi.SearchIntegrator;
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
   private final Map<String, Class<?>> indexedEntities;

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
      }
      indexedEntities = Collections.unmodifiableMap(entities);
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
