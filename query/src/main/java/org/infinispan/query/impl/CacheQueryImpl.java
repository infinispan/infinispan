package org.infinispan.query.impl;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.Explanation;
import org.hibernate.search.backend.lucene.LuceneExtension;
import org.hibernate.search.backend.lucene.search.query.LuceneSearchQuery;
import org.hibernate.search.engine.search.query.SearchQuery;
import org.hibernate.search.util.common.SearchException;
import org.infinispan.AdvancedCache;
import org.infinispan.query.FetchOptions;
import org.infinispan.query.FetchOptions.FetchMode;
import org.infinispan.query.ResultIterator;
import org.infinispan.query.core.impl.PartitionHandlingSupport;
import org.infinispan.query.dsl.embedded.impl.SearchQueryBuilder;

/**
 * Implementation class of the CacheQuery interface.
 * <p/>
 *
 * @author Navin Surtani
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2011 Red Hat Inc.
 * @author Marko Luksa
 */
public class CacheQueryImpl<E> implements IndexedQuery<E> {

   /**
    * Since CacheQuery extends {@link Iterable} it is possible to implicitly invoke
    * {@link #iterator()} in an "enhanced for loop".
    * When using the {@link FetchMode#LAZY} it is mandatory to close the {@link ResultIterator},
    * but users of the enhanced loop have no chance to invoke the method.
    * Therefore, it's important that the default fetch options use EAGER iteration.
    */
   private static final FetchOptions DEFAULT_FETCH_OPTIONS = new FetchOptions().fetchMode(FetchMode.EAGER);

   protected final AdvancedCache<?, ?> cache;
   protected final PartitionHandlingSupport partitionHandlingSupport;
   protected QueryDefinition queryDefinition;

   public CacheQueryImpl(QueryDefinition queryDefinition, AdvancedCache<?, ?> cache) {
      this.queryDefinition = queryDefinition;
      this.cache = cache;
      this.partitionHandlingSupport = new PartitionHandlingSupport(cache);
   }

   /**
    * Create a CacheQueryImpl based on a SearchQuery.
    */
   public CacheQueryImpl(SearchQueryBuilder searchQuery, AdvancedCache<?, ?> cache) {
      this(new QueryDefinition(searchQuery), cache);
   }

   /**
    * @return The result size of the query.
    */
   @Override
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

   @Override
   public ResultIterator<E> iterator() throws SearchException {
      return iterator(DEFAULT_FETCH_OPTIONS);
   }

   @Override
   public ResultIterator<E> iterator(FetchOptions fetchOptions) throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      SearchQuery<?> searchQuery = queryDefinition.getSearchQuery().build();

      // TODO HSEARCH-3323 Restore support for scrolling
      // if (fetchOptions.getFetchMode() == FetchOptions.FetchMode.LAZY) { ...

      List<E> queryHits = (List<E>) searchQuery.fetchHits(queryDefinition.getFirstResult(), queryDefinition.getMaxResults());
      return filterNulls(new EagerIterator<>(queryHits, fetchOptions.getFetchSize()));
   }

   private ResultIterator<E> filterNulls(ResultIterator<E> iterator) {
      return new NullFilteringResultIterator<>(iterator);
   }

   @Override
   public List<E> list() throws SearchException {
      partitionHandlingSupport.checkCacheAvailable();
      SearchQuery<?> searchQuery = queryDefinition.getSearchQuery().build();

      List<?> searchResult = searchQuery.fetchHits(queryDefinition.getFirstResult(), queryDefinition.getMaxResults());

      return (List<E>) searchResult;
   }

   public Explanation explain(String id) {
      LuceneSearchQuery<?> luceneSearchQuery = queryDefinition.getSearchQuery().build()
            .extension(LuceneExtension.get());
      return luceneSearchQuery.explain(id);
   }

   @Override
   public IndexedQuery<E> timeout(long timeout, TimeUnit timeUnit) {
      queryDefinition.failAfter(timeout, timeUnit);
      return this;
   }
}
