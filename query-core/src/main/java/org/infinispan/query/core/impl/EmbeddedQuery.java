package org.infinispan.query.core.impl;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.infinispan.query.core.impl.Log.CONTAINER;

import org.infinispan.AdvancedCache;
import org.infinispan.CacheStream;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.filter.CacheFilters;
import org.infinispan.objectfilter.ObjectFilter;
import org.infinispan.objectfilter.impl.syntax.parser.IckleParsingResult;
import org.infinispan.query.core.impl.eventfilter.IckleFilterAndConverter;
import org.infinispan.query.core.stats.impl.LocalQueryStatistics;
import org.infinispan.query.dsl.QueryFactory;
import org.infinispan.query.dsl.QueryResult;


/**
 * Non-indexed embedded-mode query.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public final class EmbeddedQuery<T> extends BaseEmbeddedQuery<T> {

   private final QueryEngine<?> queryEngine;

   private IckleFilterAndConverter<?, ?> filter;

   private final int defaultMaxResults;

   public EmbeddedQuery(QueryEngine<?> queryEngine, QueryFactory queryFactory, AdvancedCache<?, ?> cache,
                        String queryString, IckleParsingResult.StatementType statementType,
                        Map<String, Object> namedParameters, String[] projection,
                        long startOffset, int maxResults, int defaultMaxResults, LocalQueryStatistics queryStatistics, boolean local) {
      super(queryFactory, cache, queryString, statementType, namedParameters, projection, startOffset, maxResults, queryStatistics, local);
      this.queryEngine = queryEngine;
      this.defaultMaxResults = defaultMaxResults;

      if (maxResults == -1) {
         // apply the default
         super.maxResults = defaultMaxResults;
      }
   }

   @Override
   public void resetQuery() {
      super.resetQuery();
      filter = null;
   }

   @Override
   protected void recordQuery(long time) {
      queryStatistics.nonIndexedQueryExecuted(queryString, time);
   }

   private IckleFilterAndConverter<?, ?> createFilter() {
      // filter is created first time only, or again if resetQuery was called meanwhile
      if (filter == null) {
         filter = queryEngine.createAndWireFilter(queryString, namedParameters);

         // force early query validation, at creation time, rather than deferring to execution time
         filter.getObjectFilter();
      }
      return filter;
   }

   @Override
   protected Comparator<Comparable<?>[]> getComparator() {
      return createFilter().getObjectFilter().getComparator();
   }

   @Override
   protected CloseableIterator<ObjectFilter.FilterResult> getInternalIterator() {
      IckleFilterAndConverter<Object, Object> ickleFilter = (IckleFilterAndConverter<Object, Object>) createFilter();
      AdvancedCache<Object, Object> cache = (AdvancedCache<Object, Object>) (isLocal() ? this.cache.withFlags(Flag.CACHE_MODE_LOCAL) : this.cache);

      CacheStream<CacheEntry<Object, Object>> entryStream = cache.cacheEntrySet().stream();
      if (timeout > 0) {
         entryStream = entryStream.timeout(timeout, TimeUnit.NANOSECONDS);
      }
      CacheStream<ObjectFilter.FilterResult> resultStream = CacheFilters.filterAndConvertToValue(entryStream, ickleFilter);
      if (timeout > 0) {
         resultStream = resultStream.timeout(timeout, TimeUnit.NANOSECONDS);
      }
      return Closeables.iterator(resultStream);
   }

   @Override
   public QueryResult<T> execute() {
      if (isSelectStatement()) {
         return super.execute();
      }

      return new QueryResultImpl<>(executeStatement(), Collections.emptyList());
   }

   @Override
   public int executeStatement() {
      if (isSelectStatement()) {
         throw CONTAINER.unsupportedStatement();
      }

      if (getStartOffset() != 0 || getMaxResults() != defaultMaxResults) {
         throw CONTAINER.deleteStatementsCannotUsePaging();
      }

      IckleFilterAndConverter<Object, Object> ickleFilter = (IckleFilterAndConverter<Object, Object>) createFilter();
      AdvancedCache<Object, Object> cache = (AdvancedCache<Object, Object>) (isLocal() ? this.cache.withFlags(Flag.CACHE_MODE_LOCAL) : this.cache);

      CacheStream<CacheEntry<Object, Object>> entryStream = cache.cacheEntrySet().stream();
      if (timeout > 0) {
         entryStream = entryStream.timeout(timeout, TimeUnit.NANOSECONDS);
      }

      CacheStream<?> filteredKeyStream = CacheFilters.filterAndConvertToKey(entryStream, ickleFilter);
      if (timeout > 0) {
         filteredKeyStream = filteredKeyStream.timeout(timeout, TimeUnit.NANOSECONDS);
      }

      Optional<Integer> count = filteredKeyStream.map(new DeleteFunction()).reduce(Integer::sum);
      filteredKeyStream.close();
      return count.orElse(0);
   }

   @Scope(Scopes.NONE)
   static final class DeleteFunction implements Function<Object, Integer> {

      @Inject
      AdvancedCache<?,?> cache;

      @Override
      public Integer apply(Object key) {
         key = cache.getKeyDataConversion().fromStorage(key);
         return cache.remove(key) == null ? 0 : 1;
      }
   }

   public static final class DeleteFunctionExternalizer implements  AdvancedExternalizer<DeleteFunction> {

      @Override
      public void writeObject(ObjectOutput output, DeleteFunction object) {
      }

      @Override
      public DeleteFunction readObject(ObjectInput input) {
         return new DeleteFunction();
      }

      @Override
      public Set<Class<? extends DeleteFunction>> getTypeClasses() {
         return Collections.singleton(DeleteFunction.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.ICKLE_DELETE_FUNCTION;
      }
   };

   @Override
   public String toString() {
      return "EmbeddedQuery{" +
            "queryString=" + queryString +
            ", statementType=" + statementType +
            ", namedParameters=" + namedParameters +
            ", projection=" + Arrays.toString(projection) +
            ", startOffset=" + startOffset +
            ", maxResults=" + maxResults +
            ", defaultMaxResults=" + defaultMaxResults +
            ", timeout=" + timeout +
            '}';
   }
}
