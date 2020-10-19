package org.infinispan.query.dsl;

/**
 * Represents the result of a {@link Query}.
 *
 * @param <E> The type of the result. Queries having projections (a SELECT clause) will return an Object[] for each
 *            result, with the field values. When not using SELECT, the results will contain instances of objects
 *            corresponding to the cache values matching the query.
 * @since 11.0
 */
@Deprecated
public interface QueryResult<E> extends org.infinispan.commons.api.QueryResult<E> {
}
