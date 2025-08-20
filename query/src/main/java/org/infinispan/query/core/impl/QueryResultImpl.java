package org.infinispan.query.core.impl;

import java.util.Collections;
import java.util.List;

import org.infinispan.commons.api.query.HitCount;
import org.infinispan.commons.query.TotalHitCount;
import org.infinispan.query.dsl.QueryResult;

/**
 * @since 11.0
 */
public record QueryResultImpl<E>(HitCount count, List<E> list) implements QueryResult<E> {

   public static final QueryResult<?> EMPTY = new QueryResultImpl<>(TotalHitCount.EMPTY, Collections.emptyList());

   public QueryResultImpl(int hitCount, List<E> list) {
      this(new TotalHitCount(hitCount, true), list);
   }

}
