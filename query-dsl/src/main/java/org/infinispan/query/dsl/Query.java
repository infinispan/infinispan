package org.infinispan.query.dsl;

import java.util.List;
import java.util.Map;

//todo [anistor] We need to deprecate the 'always caching' behaviour and provide a clearCachedResults method
/**
 * An immutable object representing both the query and the result. The result is obtained lazily when one of the methods
 * in this interface is executed first time. The query is executed only once. Further calls will just return the
 * previously cached results. If you intend to re-execute the query to obtain fresh data you need to build another
 * instance using a {@link QueryBuilder}.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Query extends PaginationContext<Query>, ParameterContext<Query> {

   /**
    * Returns the Ickle query string.
    */
   String getQueryString();

   /**
    * Returns the results of a search as a list.
    *
    * @return list of objects that were found from the search.
    */
   <T> List<T> list();

   /**
    * Gets the total number of results matching the query, ignoring pagination (firstResult, maxResult).
    *
    * @return total number of results.
    */
   int getResultSize(); //todo [anistor] this should probably be a long?

   /**
    * @return the values for query projections or {@code null} if the query does not have projections.
    */
   String[] getProjection();

   long getStartOffset();

   Query startOffset(long startOffset);

   int getMaxResults();

   Query maxResults(int maxResults);

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
   Query setParameter(String paramName, Object paramValue);

   /**
    * Sets multiple named parameters at once. Parameters names cannot be empty or {@code null}. Parameter values must
    * not be {@code null}.
    *
    * @param paramValues a Map of parameters
    * @return itself
    */
   Query setParameters(Map<String, Object> paramValues);
}
