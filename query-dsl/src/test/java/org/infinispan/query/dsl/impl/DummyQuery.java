package org.infinispan.query.dsl.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryResult;

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
      return new CloseableIterator<T>() {

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
   public <K> CloseableIterator<Map.Entry<K, T>> entryIterator() {
      return new CloseableIterator<Map.Entry<K, T>>() {

         @Override
         public void close() {
         }

         @Override
         public boolean hasNext() {
            return false;
         }

         @Override
         public Map.Entry<K, T> next() {
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
         public OptionalLong hitCount() {
            return OptionalLong.empty();
         }

         @Override
         public List<T> list() {
            return Collections.emptyList();
         }
      };
   }

   @Override
   public int executeStatement() {
      return 0;
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
}
