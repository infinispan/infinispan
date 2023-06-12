package org.infinispan.query.core.impl;

import java.util.Collections;
import java.util.List;
import java.util.OptionalLong;

import org.infinispan.query.dsl.QueryResult;

/**
 * @since 11.0
 */
public final class QueryResultImpl<E> implements QueryResult<E> {

   public static final QueryResult<?> EMPTY = new QueryResultImpl<>(0, Collections.emptyList());

   private final OptionalLong hitCount;

   private final List<E> list;

   public QueryResultImpl(long hitCount, List<E> list) {
      this(OptionalLong.of(hitCount), list);
   }

   public QueryResultImpl(List<E> list) {
      this(OptionalLong.empty(), list);
   }

   public QueryResultImpl(OptionalLong hitCount, List<E> list) {
      this.hitCount = hitCount;
      this.list = list;
   }

   //todo [anistor] why not int? cache size is not a long!
   @Override
   public OptionalLong hitCount() {
      return hitCount;
   }

   @Override
   public List<E> list() {
      return list;
   }
}
