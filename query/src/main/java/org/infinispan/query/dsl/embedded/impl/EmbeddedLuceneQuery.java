package org.infinispan.query.dsl.embedded.impl;

import org.infinispan.query.CacheQuery;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.embedded.LuceneQuery;
import org.infinispan.query.dsl.impl.BaseQuery;

import java.util.List;


/**
 * A query implementation based on Lucene.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
final class EmbeddedLuceneQuery extends BaseQuery implements LuceneQuery {

   private final CacheQuery cacheQuery;

   EmbeddedLuceneQuery(QueryFactory queryFactory, String jpaQuery, CacheQuery cacheQuery) {
      super(queryFactory, jpaQuery);
      this.cacheQuery = cacheQuery;
   }

   @Override
   @SuppressWarnings("unchecked")
   public <T> List<T> list() {
      return (List<T>) cacheQuery.list();
   }

   @Override
   public ResultIterator iterator(FetchOptions fetchOptions) {
      return cacheQuery.iterator(fetchOptions);
   }

   @Override
   public ResultIterator iterator() {
      return cacheQuery.iterator();
   }

   @Override
   public int getResultSize() {
      return cacheQuery.getResultSize();
   }

   @Override
   public String toString() {
      return "EmbeddedLuceneQuery{" +
            "jpaQuery=" + jpaQuery +
            ", cacheQuery=" + cacheQuery +
            '}';
   }
}
