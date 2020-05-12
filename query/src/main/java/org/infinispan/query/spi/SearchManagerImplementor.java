package org.infinispan.query.spi;

import org.infinispan.query.CacheQuery;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;
import org.infinispan.query.impl.QueryDefinition;

/**
 * @deprecated Since 11.0, without replacement. To be removed in next major version.
 */
@Deprecated
public interface SearchManagerImplementor extends SearchManager {

   /**
    * Creates a cache query based on a {@link QueryDefinition} and a custom metadata.
    * @deprecated Since 11.0, without replacement. To be removed in next major version.
    */
   @Deprecated
   <E> CacheQuery<E> getQuery(QueryDefinition queryDefinition, IndexedQueryMode indexedQueryMode);

   @Deprecated
   <E> CacheQuery<E> getQuery(SearchQueryBuilder searchQuery);
}
