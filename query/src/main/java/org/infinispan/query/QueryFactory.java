package org.infinispan.query;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Query;
import org.hibernate.search.engine.SearchFactoryImplementor;
import org.infinispan.Cache;
import org.infinispan.query.impl.CacheQueryImpl;
import org.infinispan.query.backend.QueryHelper;

/**
 * Class that is used to build {@link org.infinispan.query.CacheQuery}
 *
 *
 *
 * @author Navin Surtani
 * @since 4.0
 */


public class QueryFactory {

   private Cache cache;
   private SearchFactoryImplementor searchFactory;

   public QueryFactory(Cache cache, QueryHelper qh){
      this.cache = cache;
      searchFactory = qh.getSearchFactory();
   }

   /**
    * This is a simple method that will just return a {@link CacheQuery}
    *
    * @param luceneQuery - {@link org.apache.lucene.search.Query}
    * @return the query result
    */

   public CacheQuery getQuery(Query luceneQuery){
      return new CacheQueryImpl(luceneQuery, searchFactory, cache);
   }

   /**
    * This method is a basic query. The user provides 2 strings and internally the {@link org.apache.lucene.search.Query} is built.
    *
    * The first string is the field that they are searching and the second one is the search that they want to run.
    *
    * For example: -
    *
    * {@link org.infinispan.query.CacheQuery} cq = new QueryFactory
    *
    * The query is built by use of a {@link org.apache.lucene.queryParser.QueryParser} and a {@link org.apache.lucene.analysis.standard.StandardAnalyzer}
    * </p>
    *
    * @param field - the field on the class that you are searching
    * @param search - the String that you want to be using to search
    * @return {@link org.infinispan.query.CacheQuery} result
    */

   public CacheQuery getBasicQuery(String field, String search) throws org.apache.lucene.queryParser.ParseException{

      QueryParser parser = new QueryParser(field, new StandardAnalyzer());
      org.apache.lucene.search.Query luceneQuery = parser.parse(search);
      return new CacheQueryImpl(luceneQuery, searchFactory, cache);

   }
}
