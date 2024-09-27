package org.infinispan.query.dsl.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.api.query.ClosableIteratorWithCount;
import org.infinispan.commons.api.query.EntityEntry;
import org.infinispan.commons.api.query.HitCount;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.dsl.TotalHitCount;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class DummyQuery<T> implements Query<T> {

   @Override
   public Map<String, Object> getParameters() {
      return null;
   }

   @Override
   public Query<T> setParameter(String paramName, Object paramValue) {
      return this;
   }

   @Override
   public Query<T> setParameters(Map<String, Object> paramValues) {
      return null;
   }

   @Override
   public CloseableIterator<T> iterator() {
      return new CloseableIterator<>() {

         @Override
         public void close() {
         }

         @Override
         public boolean hasNext() {
            return false;
         }

         @Override
         public T next() {
            throw new NoSuchElementException();
         }
      };
   }

   @Override
   public <K> ClosableIteratorWithCount<EntityEntry<K, T>> entryIterator(boolean withMetadata) {
      return new ClosableIteratorWithCount<EntityEntry<K, T>>() {

         @Override
         public HitCount count() {
            return null;
         }

         @Override
         public void close() {
         }

         @Override
         public boolean hasNext() {
            return false;
         }

         @Override
         public EntityEntry<K, T> next() {
            throw new NoSuchElementException();
         }
      };
   }

   @Override
   public Query<T> timeout(long timeout, TimeUnit timeUnit) {
      return this;
   }

   @Override
   public Query<T> local(boolean local) {
      return this;
   }

   @Override
   public Query<T> scoreRequired(boolean scoreRequired) {
      return this;
   }

   @Override
   public String getQueryString() {
      return null;
   }

   @Override
   public List<T> list() {
      return Collections.emptyList();
   }

   @Override
   public QueryResult<T> execute() {
      return new QueryResult<T>() {
         @Override
         public TotalHitCount count() {
            return TotalHitCount.EMPTY;
         }

         @Override
         public List<T> list() {
            return Collections.emptyList();
         }
      };
   }

   @Override
   public CompletionStage<org.infinispan.commons.api.query.QueryResult<T>> executeAsync() {
      return CompletableFuture.completedFuture(execute());
   }

   @Override
   public int executeStatement() {
      return 0;
   }

   @Override
   public CompletionStage<Integer> executeStatementAsync() {
      return CompletableFuture.completedFuture(0);
   }

   @Override
   public int getResultSize() {
      return 0;
   }

   @Override
   public String[] getProjection() {
      return null;
   }

   @Override
   public boolean hasProjections() {
      return false;
   }

   @Override
   public long getStartOffset() {
      return 0;
   }

   @Override
   public Query<T> startOffset(long startOffset) {
      return this;
   }

   @Override
   public int getMaxResults() {
      return Integer.MAX_VALUE;
   }

   @Override
   public Query<T> maxResults(int maxResults) {
      return this;
   }

   @Override
   public Integer hitCountAccuracy() {
      return null;
   }

   @Override
   public Query<T> hitCountAccuracy(int hitCountAccuracy) {
      return this;
   }
}
