package org.infinispan.query.dsl;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * An immutable object representing both the query and the result. The result is obtained lazily when one of the methods
 * in this interface is executed first time. The query is executed only once. Further calls will just return the
 * previously cached results.
 *
 * @deprecated use @{@link org.infinispan.commons.api.query.Query} instead
 * @author anistor@redhat.com
 * @since 6.0
 */
@Deprecated(since = "14.0", forRemoval = true)
public interface Query<T> extends org.infinispan.commons.api.query.Query<T> {

   @Override
   QueryResult<T> execute();

   /**
    * Due to Generic limitations the erased type must stay as the commons QueryResult.
    * Feel free to cast to {@link QueryResult} as needed in chained operations on the stage.
    * @return a stage when complete has the results
    */
   @Override
   CompletionStage<org.infinispan.commons.api.query.QueryResult<T>> executeAsync();

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
