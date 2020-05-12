package org.infinispan.query.spi;

import org.infinispan.query.CacheQuery;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;
import org.infinispan.query.impl.QueryDefinition;

public interface SearchManagerImplementor extends SearchManager {

   /**
    * Creates a cache query based on a {@link QueryDefinition} and a custom metadata.
    */
   <E> CacheQuery<E> getQuery(QueryDefinition queryDefinition, IndexedQueryMode indexedQueryMode);

   <E> CacheQuery<E> getQuery(SearchQueryBuilder searchQuery);
}
