package org.infinispan.query;

import org.apache.lucene.analysis.Analyzer;
import org.hibernate.search.stat.Statistics;
import org.infinispan.query.dsl.IndexedQueryMode;

/**
 * The SearchManager is the entry point to create full text queries on top of an indexed cache.
 *
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 */
public interface SearchManager {

   /**
    * Builds a {@link CacheQuery} from an Ickle query string.
    *
    * @throws org.hibernate.search.exception.SearchException if the queryString cannot be converted to an indexed query,
    *                                                        due to lack of indexes to resolve it fully or if contains
    *                                                        aggregations and grouping.
    */
   <E> CacheQuery<E> getQuery(String queryString, IndexedQueryMode indexedQueryMode);

   /**
    * Builds a {@link CacheQuery} from an Ickle query string, assuming the correct value for {@link IndexedQueryMode} to
    * query all data in the cluster
    * @param queryString the Ickle query
    * @return
    */

   <E> CacheQuery<E> getQuery(String queryString);
   /**
    * The MassIndexer can be used to rebuild the Lucene indexes from the entries stored in Infinispan.
    *
    * @return the MassIndexer component
    */

   MassIndexer getMassIndexer();

   /**
    * Get access to the Query specific statistics for this SearchManager instance
    *
    * @return The statistics.
    * @since 7.0
    */
   Statistics getStatistics();

   /**
    * Retrieve an analyzer instance by its definition name
    *
    * @param name the name of the analyzer
    * @return analyzer with the specified name
    * @throws org.hibernate.search.exception.SearchException if the definition name is unknown
    * @since 7.0
    */
   Analyzer getAnalyzer(String name);

   /**
    * Retrieves the scoped analyzer for a given class type.
    *
    * @param clazz The class for which to retrieve the analyzer.
    * @return The scoped analyzer for the specified class.
    * @throws java.lang.IllegalArgumentException in case {@code clazz == null} or the specified
    * class is not an indexed entity.
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
    * This method gives access to internal Infinispan implementation details, and should not be normally needed. The
    * interface of the internal types does not constitute a public API and can (and probably will) change without
    * notice.
    *
    * @param cls the class of the desired internal component
    * @return the 'unwrapped' internal component
    * @throws IllegalArgumentException if the class of the requested internal component is not recognized
    */
   <T> T unwrap(Class<T> cls);
}
