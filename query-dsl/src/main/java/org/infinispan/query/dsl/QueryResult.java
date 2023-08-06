package org.infinispan.query.dsl;

import java.util.OptionalLong;

/**
 * Represents the result of a {@link Query}.
 * <p>
 * If the query was executed using {@link Query#executeStatement()}, the list of results will be empty and
 * {@link #count()} will return the number of affected entries.
 *
 * @param <E> The type of the result. Queries having projections (a SELECT clause) will return an Object[] for each
 *            result, with the field values. When not using SELECT, the results will contain instances of objects
 *            corresponding to the cache values matching the query.
 * @since 11.0
 */
public interface QueryResult<E> extends org.infinispan.commons.api.query.QueryResult<E> {

   /**
    * @return The number of hits from the query, ignoring pagination. When the query is non-indexed, for performance
    * reasons, the hit count is not calculated and will return {@link OptionalLong#empty()}.
    *
    * @deprecated replaced by {@link #count()}
    */
   @Deprecated
   default OptionalLong hitCount() {
      TotalHitCount count = count();
      return count.isExact() ? OptionalLong.of(count.value()) : OptionalLong.empty();
   }

   /**
    * @return An object containing information about the number of hits from the query, ignoring pagination.
    */
   @Override
   TotalHitCount count();

}
