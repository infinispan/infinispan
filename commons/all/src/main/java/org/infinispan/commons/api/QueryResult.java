package org.infinispan.commons.api;

import java.util.List;
import java.util.OptionalLong;

/**
 * Represents the result of a {@link Query}.
 *
 * @param <E> The type of the result. Queries having projections (a SELECT clause) will return an Object[] for each
 *            result, with the field values. When not using SELECT, the results will contain instances of objects
 *            corresponding to the cache values matching the query.
 * @since 12.0
 */
public interface QueryResult<E> {

   /**
    * @return The number of hits from the query, ignoring pagination. When the query is non-indexed, for performance
    * reasons, the hit count is not calculated and will return {@link OptionalLong#empty()}.
    */
   OptionalLong hitCount();

   /**
    * @return The results of the query as a list, respecting the bounds specified in {@link Query#startOffset(long)} and
    * {@link Query#maxResults(int)}.
    */
   List<E> list();
}
