package org.infinispan.objectfilter;

import org.infinispan.query.dsl.Query;

/**
 * An object matcher able to test a given object against multiple registered filters specified either as JPA queries or
 * using the query DSL (see {@link org.infinispan.query.dsl}). The matching filters are notified via a callback supplied
 * when registering the filter. The filter will have to specify the fully qualified type name of the matching
 * object/entity because simple names cannot be resolved as it would happen in the case of an {@link
 * javax.persistence.EntityManager} which has knowledge of all types in advance.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface Matcher {

   FilterSubscription registerFilter(Query query, FilterCallback callback, Object... eventType);

   FilterSubscription registerFilter(String jpaQuery, FilterCallback callback, Object... eventType);

   void unregisterFilter(FilterSubscription filterSubscription);

   /**
    * Test the given instance against all the subscribed filters and notify all callbacks registered for instances of
    * the same type.
    *
    * @param userContext an optional user provided object to be passed to matching subscribers along with the matching
    *                    instance; can be {@code null}
    * @param instance    the object to test against the registered filters; never {@code null}
    * @param eventType   on optional event type discriminator that is matched against the even type specified when the
    *                    filter was registered; can be {@code null}
    */
   void match(Object userContext, Object instance, Object eventType);

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
