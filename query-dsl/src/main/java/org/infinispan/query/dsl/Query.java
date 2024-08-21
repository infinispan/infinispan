package org.infinispan.query.dsl;

//todo [anistor] We need to deprecate the 'always caching' behaviour and provide a clearCachedResults method

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * An immutable object representing both the query and the result. The result is obtained lazily when one of the methods
 * in this interface is executed first time. The query is executed only once. Further calls will just return the
 * previously cached results. If you intend to re-execute the query to obtain fresh data you need to build another
 * instance using a {@link QueryBuilder}.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Query<T> extends org.infinispan.commons.api.query.Query<T> {

   @Override
   QueryResult<T> execute();

   /**
    * Gets the total number of results matching the query, ignoring pagination (startOffset, maxResults).
    *
    * @return total number of results.
    * @deprecated since 10.1. This will be removed in 12. It's closest replacement is {@link QueryResult#hitCount()}
    * which returns an optional long.
    */
   @Deprecated
   int getResultSize();

   /**
    * @return the values for query projections or {@code null} if the query does not have projections.
    * @deprecated since 11.0. This method will be removed in next major version. To find out if a query uses projections use {@link #hasProjections()}
    */
   @Deprecated
   String[] getProjection();

   @Override
   Query<T> startOffset(long startOffset);

   @Override
   Query<T> maxResults(int maxResults);

   @Override
   Query<T> hitCountAccuracy(int hitCountAccuracy);

   @Override
   Query<T> setParameter(String paramName, Object paramValue);

   @Override
   Query<T> setParameters(Map<String, Object> paramValues);

   @Override
   Query<T> timeout(long timeout, TimeUnit timeUnit);

}
