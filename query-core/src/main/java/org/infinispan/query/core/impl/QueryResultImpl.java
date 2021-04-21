package org.infinispan.query.core.impl;

import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import org.infinispan.query.dsl.QueryResult;

/**
 * @since 11.0
 */
public final class QueryResultImpl<E> implements QueryResult<E> {

   private final OptionalLong hitCount;

   public static final QueryResult<?> EMPTY = new QueryResultImpl<>(0, Collections.emptyList());

   private final List<E> list;

   public QueryResultImpl(long hitCount, List<E> list) {
      this.hitCount = OptionalLong.of(hitCount);
      this.list = list;
   }

   public QueryResultImpl(List<E> list) {
      this.hitCount = OptionalLong.empty();
      this.list = list;
   }

   @Override
   public OptionalLong hitCount() {
      return hitCount;
   }

   @Override
   public List<E> list() {
      return list;
   }
}
