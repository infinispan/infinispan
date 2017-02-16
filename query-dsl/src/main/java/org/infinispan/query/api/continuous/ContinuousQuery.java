package org.infinispan.query.api.continuous;

import java.util.List;
import java.util.Map;

import org.infinispan.query.dsl.Query;

/**
 * A container of continuous query listeners for a cache.
 * <p>
 * Implementations are not expected to be threadsafe.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
public interface ContinuousQuery<K, V> {

   /**
    * Add a listener for a continuous query.
    *
    * @param queryString the query
    * @param listener    the listener
    */
   <C> void addContinuousQueryListener(String queryString, ContinuousQueryListener<K, C> listener);

   /**
    * Add a listener for a continuous query.
    *
    * @param queryString     the query
    * @param namedParameters the query parameters
    * @param listener        the listener
    */
   <C> void addContinuousQueryListener(String queryString, Map<String, Object> namedParameters, ContinuousQueryListener<K, C> listener);

   /**
    * Add a listener for a continuous query.
    *
    * @param query    the query object
    * @param listener the listener
    */
   <C> void addContinuousQueryListener(Query query, ContinuousQueryListener<K, C> listener);

   /**
    * Remove a continuous query listener.
    *
    * @param listener the listener to remove
    */
   void removeContinuousQueryListener(ContinuousQueryListener<K, ?> listener);

   /**
    * Get the list of currently registered listeners.
    */
   List<ContinuousQueryListener<K, ?>> getListeners();

   /**
    * Unregisters all listeners.
    */
   void removeAllListeners();
}
