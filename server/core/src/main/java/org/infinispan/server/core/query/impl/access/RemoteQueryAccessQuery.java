package org.infinispan.server.core.query.impl.access;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.api.query.ClosableIteratorWithCount;
import org.infinispan.commons.api.query.EntityEntry;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.server.core.query.impl.RemoteQueryAccessEngine;

public class RemoteQueryAccessQuery<T> implements Query<T> {

   private final RemoteQueryAccessEngine engine;
   private final String queryString;

   private Map<String, Object> namedParametersMap = new HashMap<>();
   private int offset = 0;
   private int maxResults;
   private int hitCountAccuracy;
   public boolean local = false;

   public RemoteQueryAccessQuery(RemoteQueryAccessEngine engine, String queryString) {
      this.engine = engine;
      this.queryString = queryString;
      this.maxResults = engine.defaultMaxResult();
      this.hitCountAccuracy = engine.defaultHitCountAccuracy();
   }

   @Override
   public String getQueryString() {
      return queryString;
   }

   @Override
   public List<T> list() {
      return execute().list();
   }

   @Override
   public QueryResult<T> execute() {
      return (QueryResult<T>) engine.executeQuery(
            queryString, namedParametersMap, offset, maxResults, hitCountAccuracy, local);
   }

   @Override
   public CompletionStage<QueryResult<T>> executeAsync() {
      throw new UnsupportedOperationException();
   }

   @Override
   public int executeStatement() {
      throw new UnsupportedOperationException("Not implemented!");
   }

   @Override
   public CompletionStage<Integer> executeStatementAsync() {
      throw new UnsupportedOperationException("Not implemented!");
   }

   @Override
   public boolean hasProjections() {
      throw new UnsupportedOperationException("Not implemented!");
   }

   @Override
   public long getStartOffset() {
      return offset;
   }

   @Override
   public Query<T> startOffset(long startOffset) {
      this.offset = (int) startOffset;
      return this;
   }

   @Override
   public int getMaxResults() {
      return maxResults;
   }

   @Override
   public Query<T> maxResults(int maxResults) {
      this.maxResults = maxResults;
      return this;
   }

   @Override
   public Integer hitCountAccuracy() {
      return hitCountAccuracy;
   }

   @Override
   public Query<T> hitCountAccuracy(int hitCountAccuracy) {
      this.hitCountAccuracy = hitCountAccuracy;
      return this;
   }

   @Override
   public Map<String, Object> getParameters() {
      return namedParametersMap;
   }

   @Override
   public Query<T> setParameter(String paramName, Object paramValue) {
      this.namedParametersMap.put(paramName, paramValue);
      return this;
   }

   @Override
   public Query<T> setParameters(Map<String, Object> paramValues) {
      this.namedParametersMap = paramValues;
      return this;
   }

   @Override
   public CloseableIterator<T> iterator() {
      throw new UnsupportedOperationException("Not implemented!");
   }

   @Override
   public <K> ClosableIteratorWithCount<EntityEntry<K, T>> entryIterator(boolean withMetadata) {
      throw new UnsupportedOperationException("Not implemented!");
   }

   @Override
   public Query<T> timeout(long timeout, TimeUnit timeUnit) {
      throw new UnsupportedOperationException("Not implemented!");
   }

   @Override
   public Query<T> local(boolean local) {
      this.local = local;
      return this;
   }

   @Override
   public Query<T> scoreRequired(boolean scoreRequired) {
      throw new UnsupportedOperationException("Not implemented!");
   }
}
