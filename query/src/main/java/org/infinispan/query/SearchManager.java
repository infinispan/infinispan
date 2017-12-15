package org.infinispan.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.EntityContext;
import org.hibernate.search.stat.Statistics;
import org.infinispan.query.dsl.IndexedQueryMode;

/**
 * The SearchManager is the entry point to create full text queries on top of a cache.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 */
public interface SearchManager {

   /**
    * This is a simple method that will just return a {@link CacheQuery}, filtered according to a set of classes passed
    * in.  If no classes are passed in, it is assumed that no type filtering is performed and so all known types will
    * be searched.
    *
    * @param luceneQuery      {@link org.apache.lucene.search.Query}
    * @param indexedQueryMode The {@link IndexedQueryMode} used when executing the query.
    * @param classes          Optionally only return results of type that matches this list of acceptable types.
    * @return the CacheQuery object which can be used to iterate through results.
    */
   <E> CacheQuery<E> getQuery(Query luceneQuery, IndexedQueryMode indexedQueryMode, Class<?>... classes);

   /**
    * Builds a {@link CacheQuery} from a query string.
    *
    * @throws org.hibernate.search.exception.SearchException if the queryString cannot be converted to an indexed query,
    *                                                        due to lack of indexes to resolve it fully or if contains
    *                                                        aggregations and grouping.
    * @see #getQuery(Query, IndexedQueryMode, Class[])
    */
   <E> CacheQuery<E> getQuery(String queryString, IndexedQueryMode indexedQueryMode, Class<?>... classes);

   /**
    * @see #getQuery(Query, IndexedQueryMode, Class[])
    */
   <E> CacheQuery<E> getQuery(Query luceneQuery, Class<?>... classes);

   /**
    * @return {@link EntityContext}
    */
   EntityContext buildQueryBuilderForClass(Class<?> entityType);

   /**
    * @param luceneQuery
    * @param classes
    * @return
    * @deprecated since 9.2, use {@link #getQuery(Query, IndexedQueryMode, Class[])} with QueryMode.BROADCAST
    */
   @Deprecated
   <E> CacheQuery<E> getClusteredQuery(Query luceneQuery, Class<?>... classes);

   /**
    * The MassIndexer can be used to rebuild the Lucene indexes from
    * the entries stored in Infinispan.
    * @return the MassIndexer component
    */
   MassIndexer getMassIndexer();

   /**
    * Retrieve an analyzer instance by its definition name
    *
    * @param name the name of the analyzer
    *
    * @return analyzer with the specified name
    *
    * @throws org.hibernate.search.exception.SearchException if the definition name is unknown
    * @since 7.0
    */
   Analyzer getAnalyzer(String name);

   /**
    * Get access to the Query specific statistics for this SearchManager instance
    *
    * @return The statistics.
    * @since 7.0
    */
   Statistics getStatistics();

   /**
    * Retrieves the scoped analyzer for a given class type.
    *
    * @param clazz The class for which to retrieve the analyzer.
    *
    * @return The scoped analyzer for the specified class.
    *
    * @throws java.lang.IllegalArgumentException in case {@code clazz == null} or the specified
    * class is not an indexed entity.
    *
    * @since 7.0
    */
   Analyzer getAnalyzer(Class<?> clazz);

   /**
    * Remove all entities of particular class from the index.
    *
    * @param entityType The class of the entity to remove.
    */
   void purge(Class<?> entityType);

   /**
    * This method gives access to internal Infinispan types, and should not be normally needed.
    * The API of the internal types can (and probably will) change without notice.
    */
   <T> T unwrap(Class<T> cls);

}
