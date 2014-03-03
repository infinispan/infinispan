package org.infinispan.query.dsl;

import java.util.List;

/**
 * An immutable object representing both the query and the result. The result is obtained lazily when one of the methods
 * in this interface is executed first time. The query is executed only once. Further calls will just return the
 * previously cached results. If you intend to re-execute the query to obtain fresh data you need to build another
 * instance using a {@link QueryBuilder}.
 *
 * @author anistor@redhat.com
 * @since 6.0
 */
public interface Query {

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

   //todo [anistor] also add long getStartOffset() ?
}
