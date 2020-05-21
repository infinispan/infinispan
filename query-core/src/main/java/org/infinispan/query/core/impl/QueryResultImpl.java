package org.infinispan.query.core.impl;

import java.util.List;
import java.util.OptionalLong;

import org.infinispan.query.dsl.QueryResult;

/**
 * @since 11.0
 */
public final class QueryResultImpl<E> implements QueryResult<E> {
   private final OptionalLong hitCount;
   private final List<E> list;

   public QueryResultImpl(OptionalLong hitCount, List<E> list) {
      this.hitCount = hitCount;
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
