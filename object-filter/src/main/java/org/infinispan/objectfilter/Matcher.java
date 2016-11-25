package org.infinispan.objectfilter;

import java.util.List;
import java.util.Map;

import org.infinispan.objectfilter.impl.aggregation.FieldAccumulator;
import org.infinispan.query.dsl.Query;

/**
 * An object matcher able to test a given object against multiple registered filters specified either as Ickle queries
 * (a JP-QL subset with full-text support) or using the query DSL (see {@link org.infinispan.query.dsl}). The matching
 * filters are notified via a callback supplied when registering the filter. The filter will have to specify the fully
 * qualified type name of the matching entity type because simple names cannot be easily resolved as it would happen
 * in the case of an {@link javax.persistence.EntityManager} which has knowledge of all types in advance and is able to
 * translate simple names to fully qualified names unambiguously.
 * <p>
 * Full-text predicates are not supported at the moment.
 * <p>
 * This is a low-level interface which should not be directly used by Infinispan users.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
public interface Matcher {

   FilterSubscription registerFilter(Query query, FilterCallback callback, Object... eventType);

   FilterSubscription registerFilter(String queryString, Map<String, Object> namedParameters, FilterCallback callback, Object... eventType);

   FilterSubscription registerFilter(String queryString, FilterCallback callback, Object... eventType);

   FilterSubscription registerFilter(Query query, FilterCallback callback, boolean isDeltaFilter, Object... eventType);

   FilterSubscription registerFilter(String queryString, Map<String, Object> namedParameters, FilterCallback callback, boolean isDeltaFilter, Object... eventType);

   FilterSubscription registerFilter(String queryString, FilterCallback callback, boolean isDeltaFilter, Object... eventType);

   void unregisterFilter(FilterSubscription filterSubscription);

   /**
    * Test the given instance against all the subscribed filters and notify all callbacks registered for instances of
    * the same instance type. The {@code isDelta} parameter of the callback will be {@code false}.
    *
    * @param userContext an optional user provided object to be passed to matching subscribers along with the matching
    *                    instance; can be {@code null}
    * @param eventType   on optional event type discriminator that is matched against the event type specified when the
    *                    filter was registered; can be {@code null}
    * @param instance    the object to test against the registered filters; never {@code null}
    */
   void match(Object userContext, Object eventType, Object instance);

   /**
    * Test two instances (which are actually before/after snapshots of the same instance) against all the subscribed
    * filters and notify all callbacks registered for instances of the same instance type. The {@code isDelta} parameter
    * of the callback will be {@code true}.
    *
    * @param userContext an optional user provided object to be passed to matching subscribers along with the matching
    *                    instance; can be {@code null}
    * @param eventType   on optional event type discriminator that is matched against the event type specified when the
    *                    filter was registered; can be {@code null}
    * @param instanceOld the 'before' object to test against the registered filters; never {@code null}
    * @param instanceNew the 'after' object to test against the registered filters; never {@code null}
    * @param joinEvent   the event to generate if the instance joins the matching set
    * @param updateEvent the event to generate if a matching instance is updated and continues to the match
    * @param leaveEvent  the event to generate if the instance leaves the matching set
    */
   void matchDelta(Object userContext, Object eventType, Object instanceOld, Object instanceNew, Object joinEvent, Object updateEvent, Object leaveEvent);

   /**
    * Obtains an ObjectFilter instance that is capable of testing a single filter condition.
    *
    * @param filterSubscription a filter subscription previously registered with this Matcher; the newly created
    *                           ObjectFilter will be based on the same filter condition
    * @return the single-filter
    */
   ObjectFilter getObjectFilter(FilterSubscription filterSubscription);

   ObjectFilter getObjectFilter(Query query);

   ObjectFilter getObjectFilter(String queryString);

   ObjectFilter getObjectFilter(String queryString, List<FieldAccumulator> acc);
}
