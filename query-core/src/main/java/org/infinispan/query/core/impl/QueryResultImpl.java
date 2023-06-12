package org.infinispan.query.core.impl;

import java.util.Collections;
import java.util.List;

import org.infinispan.query.dsl.QueryResult;
import org.infinispan.query.dsl.TotalHitCount;

/**
 * @since 11.0
 */
public final class QueryResultImpl<E> implements QueryResult<E> {

   public static final QueryResult<?> EMPTY = new QueryResultImpl<>(TotalHitCount.EMPTY, Collections.emptyList());

   private final TotalHitCount count;
   private final List<E> list;

   public QueryResultImpl(int hitCount, List<E> list) {
      this(new TotalHitCount(hitCount, true), list);
   }

   public QueryResultImpl(TotalHitCount count, List<E> list) {
      this.count = count;
      this.list = list;
   }

   @Override
   public TotalHitCount count() {
      return count;
   }

   @Override
   public List<E> list() {
      return list;
   }
}
