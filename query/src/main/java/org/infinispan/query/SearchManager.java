package org.infinispan.query;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.EntityContext;
import org.hibernate.search.spi.SearchFactoryIntegrator;
import org.hibernate.search.stat.Statistics;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.LuceneQuery;

/**
 * The SearchManager is the entry point to create full text queries on top of a cache.
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 */
public interface SearchManager {

   /**
    * Experimental! Obtains the factory for DSL-based queries backed by Lucene indexes.
    *
    * @return a factory capable of building queries for the cache this SearchManager belongs to
    * @deprecated see {@link Search#getQueryFactory}
    */
   QueryFactory<LuceneQuery> getQueryFactory();

   /**
    * This is a simple method that will just return a {@link CacheQuery}, filtered according to a set of classes passed
    * in.  If no classes are passed in, it is assumed that no type filtering is performed and so all known types will
    * be searched.
    *
    * @param luceneQuery - {@link org.apache.lucene.search.Query}
    * @param classes - optionally only return results of type that matches this list of acceptable types
    * @return the CacheQuery object which can be used to iterate through results
    */
   CacheQuery getQuery(Query luceneQuery, Class<?>... classes);

   /**
    * Experimental.
    * Provides Hibernate Search DSL to build full text queries
    * @return
    */
   EntityContext buildQueryBuilderForClass(Class<?> entityType);

   /**
    * Experimental.
    * Access the SearchFactory
    * @Deprecated This method is going to be changed or removed with no replacement.
    * If you need this method, please let us know so that replacements can be created.
    */
   SearchFactoryIntegrator getSearchFactory();

   /**
    * Experimental!
    * Use it to try out the newly introduced distributed queries.
    *
    * @param luceneQuery
    * @param classes
    * @return
    */
   CacheQuery getClusteredQuery(Query luceneQuery, Class<?>... classes);

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

}
