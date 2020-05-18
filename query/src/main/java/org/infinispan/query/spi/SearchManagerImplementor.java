package org.infinispan.query.spi;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.engine.spi.TimeoutExceptionFactory;
import org.hibernate.search.spi.CustomTypeMetadata;
import org.hibernate.search.spi.IndexedTypeMap;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.SearchManager;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.impl.QueryDefinition;

public interface SearchManagerImplementor extends SearchManager {

   /**
    * Define the timeout exception factory to customize the exception thrown when the query timeout is exceeded.
    *
    * @param timeoutExceptionFactory the timeout exception factory to use
    * @deprecated Since 11.0, without replacement.
    */
   @Deprecated
   void setTimeoutExceptionFactory(TimeoutExceptionFactory timeoutExceptionFactory);

   /**
    * Creates a cache query based on a {@link QueryDefinition} and a custom metadata.
    */
   <E> CacheQuery<E> getQuery(QueryDefinition queryDefinition, IndexedQueryMode indexedQueryMode, IndexedTypeMap<CustomTypeMetadata> indexedTypeMap);

   /**
    * This is a simple method that will just return a {@link CacheQuery}, filtered according to a set of classes passed
    * in.  If no classes are passed in, it is assumed that no type filtering is performed and so all known types will
    * be searched.
    *
    * @param luceneQuery      {@link org.apache.lucene.search.Query}
    * @param indexedQueryMode The {@link IndexedQueryMode} used when executing the query.
    * @param entity          The entity type to query against.
    * @return the CacheQuery object which can be used to iterate through results.
    */
   <E> CacheQuery<E> getQuery(Query luceneQuery, IndexedQueryMode indexedQueryMode, Class<?> entity);

}
