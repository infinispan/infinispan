package org.infinispan.query.dsl;

import java.util.List;

/**
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
   int getResultSize();
}
