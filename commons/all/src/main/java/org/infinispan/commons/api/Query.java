package org.infinispan.commons.api;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.CloseableIterator;

/**
 * An immutable object representing both the query and the result. The result is obtained lazily when one of the methods
 * in this interface is executed first time. The query is executed only once. Further calls will just return the
 * previously cached results. If you intend to re-execute the query to obtain fresh data you need to build another
 * instance using {@link QueryableCache#query(String)}
 *
 * @author anistor@redhat.com
 * @since 12.0
 */
public interface Query<T> extends Iterable<T> {

   /**
    * Returns the Ickle query string.
    */
   String getQueryString();

   /**
    *  Executes the query. Subsequent invocations cause the query to be re-executed.
    *
    * @return {@link QueryResult} with the results.
    */
   QueryResult<T> execute();

   /**
    * Indicates if the parsed query has projections (a SELECT clause) and consequently the returned results will
    * actually be {@code Object[]} containing the projected values rather than the target entity.
    *
    * @return {@code true} if it has projections, {@code false} otherwise.
    */
   boolean hasProjections();

   long getStartOffset();

   Query<T> startOffset(long startOffset);

   int getMaxResults();

   Query<T> maxResults(int maxResults);

   /**
    * Returns the named parameters Map.
    *
    * @return the named parameters (unmodifiable) or {@code null} if the query does not have parameters
    */
   Map<String, Object> getParameters();

   /**
    * Sets the value of a named parameter.
    *
    * @param paramName  the parameters name (non-empty and not null)
    * @param paramValue a non-null value
    * @return itself
    */
   Query<T> setParameter(String paramName, Object paramValue);

   /**
    * Sets multiple named parameters at once. Parameters names cannot be empty or {@code null}. Parameter values must
    * not be {@code null}.
    *
    * @param paramValues a Map of parameters
    * @return itself
    */
   Query<T> setParameters(Map<String, Object> paramValues);

   /**
    * Returns a {@link CloseableIterator} over the results. Please close the iterator when you are done with processing
    * the results.
    *
    * @return the results of the query as an iterator.
    */
   CloseableIterator<T> iterator();

   /**
    *  Set the timeout for this query. If the query hasn't finished processing before the timeout,
    *  a {@link org.infinispan.commons.TimeoutException} will be thrown.
    */
   Query<T> timeout(long timeout, TimeUnit timeUnit);
}
