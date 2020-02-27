package org.infinispan.search.mapper.session;

import java.util.Collection;
import java.util.Collections;

import org.hibernate.search.engine.search.query.dsl.SearchQuerySelectStep;
import org.hibernate.search.mapper.pojo.work.spi.PojoIndexer;
import org.infinispan.search.mapper.common.EntityReference;
import org.infinispan.search.mapper.scope.SearchScope;

/**
 * @author Fabio Massimo Ercoli
 */
public interface SearchSession extends AutoCloseable {

   /**
    * Initiate the building of a search query.
    * <p>
    * The query will target the indexes in the given scope.
    *
    * @param scope A scope representing all indexed types that will be targeted by the search query.
    * @param <E>   An entity type to include in the scope.
    * @return The initial step of a DSL where the search query can be defined.
    * @see SearchQuerySelectStep
    */
   <E> SearchQuerySelectStep<?, EntityReference, E, ?, ?, ?> search(SearchScope<E> scope);

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
    * @param <E>   A supertype of all indexed entity types that will be targeted by the search query.
    * @return The created scope.
    * @see SearchScope
    */
   <E> SearchScope<E> scope(Collection<? extends Class<? extends E>> types);

   /**
    * Create a {@link SearchScope} limited to entity types referenced by their name.
    *
    * @param expectedSuperType A supertype of all entity types to include in the scope.
    * @param entityName        An entity name.
    * @param <T>               A supertype of all entity types to include in the scope.
    * @return The created scope.
    * @see SearchScope
    */
   default <T> SearchScope<T> scope(Class<T> expectedSuperType, String entityName) {
      return scope(expectedSuperType, Collections.singleton(entityName));
   }

   /**
    * Create a {@link SearchScope} limited to entity types referenced by their name.
    *
    * @param expectedSuperType A supertype of all entity types to include in the scope.
    * @param entityNames       A collection of entity names.
    * @param <T>               A supertype of all entity types to include in the scope.
    * @return The created scope.
    * @see SearchScope
    */
   <T> SearchScope<T> scope(Class<T> expectedSuperType, Collection<String> entityNames);

   PojoIndexer createIndexer();

}
