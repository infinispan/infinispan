package org.infinispan.query.dsl;

/**
 * Factory for query objects.
 *
 * @author anistor@redhat.com
 * @since 6.0
 * @deprecated use {@link org.infinispan.commons.api.BasicCache#query(String)} instead
 */
@Deprecated(since = "15.0", forRemoval = true)
public interface QueryFactory {

   /**
    * Creates a Query based on an Ickle query string.
    *
    * @return a query
    * @deprecated use {@link org.infinispan.commons.api.BasicCache#query(String)} instead
    */
   @Deprecated
   <T> Query<T> create(String queryString);
}
