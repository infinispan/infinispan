package org.infinispan.query.impl;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.hibernate.search.engine.search.query.SearchQuery;
import org.hibernate.search.util.common.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.FilterIterator;
import org.infinispan.query.core.impl.PartitionHandlingSupport;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;

/**
 * Implementation class of the Lucene based query interface.
 * <p/>
 *
 * @author Navin Surtani
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 */
public class IndexedQueryImpl<E> implements IndexedQuery<E> {

   protected final AdvancedCache<?, ?> cache;
   protected final PartitionHandlingSupport partitionHandlingSupport;
   protected QueryDefinition queryDefinition;

   public IndexedQueryImpl(QueryDefinition queryDefinition, AdvancedCache<?, ?> cache) {
      this.queryDefinition = queryDefinition;
      this.cache = cache;
      this.partitionHandlingSupport = new PartitionHandlingSupport(cache);
   }

   /**
    * Create a CacheQueryImpl based on a SearchQuery.
    */
   public IndexedQueryImpl(SearchQueryBuilder searchQuery, AdvancedCache<?, ?> cache) {
      this(new QueryDefinition(searchQuery), cache);
   }

   /**
    * @return The result size of the query.
    */
   public int getResultSize() {
      partitionHandlingSupport.checkCacheAvailable();
      return Math.toIntExact(queryDefinition.getSearchQuery().build().fetchTotalHitCount());
   }

   /**
    * Sets the the result of the given integer value to the first result.
    *
    * @param firstResult index to be set.
    * @throws IllegalArgumentException if the index given is less than zero.
    */
   @Override
   public IndexedQuery<E> firstResult(int firstResult) {
      queryDefinition.setFirstResult(firstResult);
      return this;
   }

   @Override
   public IndexedQuery<E> maxResults(int maxResults) {
      queryDefinition.setMaxResults(maxResults);
      return this;
   }

   public CloseableIterator<E> iterator() throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      SearchQuery<?> searchQuery = queryDefinition.getSearchQuery().build();

      List<E> queryHits = (List<E>) searchQuery.fetchHits(queryDefinition.getFirstResult(), queryDefinition.getMaxResults());
      return new FilterIterator<>(queryHits.iterator(), Objects::nonNull);
   }

   @Override
   public List<E> list() throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      SearchQuery<?> searchQuery = queryDefinition.getSearchQuery().build();

      List<?> searchResult = searchQuery.fetchHits(queryDefinition.getFirstResult(), queryDefinition.getMaxResults());

      return (List<E>) searchResult;
   }

   @Override
   public IndexedQuery<E> timeout(long timeout, TimeUnit timeUnit) {
      queryDefinition.failAfter(timeout, timeUnit);
      return this;
   }
}
