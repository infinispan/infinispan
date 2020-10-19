package org.infinispan.commons.api;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 12.0
 **/
public interface QueryableCache {
   /**
    * Creates a Query based on an Ickle query string.
    *
    * @return a query
    */
   <T> Query<T> query(String queryString);
}
