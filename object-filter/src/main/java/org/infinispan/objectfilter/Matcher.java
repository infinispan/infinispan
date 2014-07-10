package org.infinispan.objectfilter;

import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;

/**
 * A matcher able to test a given object against multiple registered filters specified either as JPA queries or using
 * the query DSL (see {@link org.infinispan.query.dsl}). The matching filters are notified via a callback supplied when
 * registering the filter. The filter will have to specify the fully qualified type name of the matching object because
 * simple names cannot be easily resolved as it would happen in the case of an EntityManager that has knowledge of all
 * types in advance.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface Matcher {

   /**
    * Creates a QueryFactory capable of creating DSL based queries that are accepted by this Matcher instance as
    * arguments for the registerFilter and getObjectFilter methods.
    *
    * @return the DSL based query factory
    */
   QueryFactory<Query> getQueryFactory();

   FilterSubscription registerFilter(Query query, FilterCallback callback);

   FilterSubscription registerFilter(String jpaQuery, FilterCallback callback);

   void unregisterFilter(FilterSubscription filterSubscription);

   /**
    * Test the given instance against all the subscribed filters and notify all callbacks registered for instances of
    * the same type.
    *
    * @param instance the object to test against the registered filters; never null
    */
   void match(Object instance);

   /**
    * Obtains an ObjectFilter instance that is capable of testing a single filter condition.
    *
    * @param filterSubscription a filter subscription previously registered with this Matcher; the newly created
    *                           ObjectFilter will be based on the same filter condition
    * @return the single-filter
    */
   ObjectFilter getObjectFilter(FilterSubscription filterSubscription);

   ObjectFilter getObjectFilter(Query query);

   ObjectFilter getObjectFilter(String jpaQuery);
}
