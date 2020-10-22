package org.infinispan.search.mapper.mapping;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.hibernate.search.engine.reporting.FailureHandler;
import org.hibernate.search.mapper.pojo.model.spi.PojoBootstrapIntrospector;
import org.infinispan.search.mapper.scope.SearchScope;
import org.infinispan.search.mapper.session.SearchSession;
import org.infinispan.search.mapper.work.SearchIndexer;

public interface SearchMapping extends AutoCloseable {

   /**
    * Create a {@link SearchScope} limited to the given type.
    *
    * @param type A type to include in the scope.
    * @param <E>  An entity type to include in the scope.
    * @return The created scope.
    * @see SearchScope
    */
   default <E> SearchScope<E> scope(Class<E> type) {
      return scope(Collections.singleton(type));
   }

   /**
    * Create a {@link SearchScope} limited to the given types.
    *
    * @param types A collection of types to include in the scope.
    * @param <E>   An entity to include in the scope.
    * @return The created scope.
    * @see SearchScope
    */
   <E> SearchScope<E> scope(Collection<? extends Class<? extends E>> types);

   SearchScope<?> scopeAll();

   SearchScope<?> scopeFromJavaClasses(Collection<Class<?>> types);

   FailureHandler getFailureHandler();

   @Override
   void close();

   boolean isClose();

   SearchSession getMappingSession();

   SearchIndexer getSearchIndexer();

   /**
    * @param entityType The type of an possible-indexed entity.
    * @return A {@link SearchIndexedEntity} for the indexed entity with the exact given type,
    *         if the type matches some indexed entity, otherwise {@code null}.
    */
   SearchIndexedEntity indexedEntity(Class<?> entityType);

   SearchIndexedEntity indexedEntity(String entityName);

   //TODO: ISPN-12449 remove this method as it does not correctly return protobuf types
   Map<String, Class<?>> allIndexedTypes();

   Map<String, Class<?>> getEntities();

   boolean isIndexedType(Object value);

   static SearchMappingBuilder builder(PojoBootstrapIntrospector introspector, ClassLoader aggregatedClassLoader,
                                       Collection<ProgrammaticSearchMappingProvider> mappingProviders) {
      return new SearchMappingBuilder(introspector, aggregatedClassLoader, mappingProviders);
   }
}
