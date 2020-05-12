package org.infinispan.query;

import org.infinispan.query.dsl.IndexedQueryMode;

/**
 * The SearchManager is the entry point to create full text queries on top of an indexed cache.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 * @deprecated since 11.0, all search operations should be done with the {@link Search} entry point.
 */
@Deprecated
public interface SearchManager {

   /**
    * Builds a {@link CacheQuery} from an Ickle query string.
    *
    * @throws org.hibernate.search.util.common.SearchException if the queryString cannot be converted to an indexed query,
    *                                                        due to lack of indexes to resolve it fully or if contains
    *                                                        aggregations and grouping.
    * @deprecated since 11.0 with no replacement. To be removed in next major version.
    */
   @Deprecated
   <E> CacheQuery<E> getQuery(String queryString, IndexedQueryMode indexedQueryMode);

   /**
    * Builds a {@link CacheQuery} from an Ickle query string, assuming the correct value for {@link IndexedQueryMode} to
    * query all data in the cluster
    * @param queryString the Ickle query
    * @return
    * @deprecated since 11.0, all search operations should be done with the {@link Search} entry point.
    */
   <E> CacheQuery<E> getQuery(String queryString);

   /**
    * The MassIndexer can be used to rebuild the Lucene indexes from the entries stored in Infinispan.
    *
    * @return the MassIndexer component
    * @deprecated Since 11.0, use {@link Search#getIndexer(Cache)} instead.
    */
   @Deprecated
   MassIndexer getMassIndexer();

   // TODO HSEARCH-3129 Restore support for statistics
   // Statistics getStatistics();

   /**
    * Remove all entities of particular class from the index.
    *
    * @param entityType The class of the entity to remove.
    * @deprecated Since 11.0, use {@link Indexer#remove()} obtained from {@link Search#getIndexer(Cache)}.
    */
   @Deprecated
   void purge(Class<?> entityType);

   /**
    * This method gives access to internal Infinispan implementation details, and should not be normally needed. The
    * interface of the internal types does not constitute a public API and can (and probably will) change without
    * notice.
    *
    * @param cls the class of the desired internal component
    * @return the 'unwrapped' internal component
    * @throws IllegalArgumentException if the class of the requested internal component is not recognized
    * @deprecated Since 11.0, with no replacement.
    */
   @Deprecated
   <T> T unwrap(Class<T> cls);
}
