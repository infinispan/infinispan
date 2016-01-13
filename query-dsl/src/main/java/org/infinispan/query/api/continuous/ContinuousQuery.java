package org.infinispan.query.api.continuous;

import org.infinispan.query.dsl.Query;

import java.util.List;

/**
 * A container of continuous query listeners for a cache.
 * <p>Implementations are not expected to be threadsafe.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
public interface ContinuousQuery<K, V> {

   <C> void addContinuousQueryListener(Query query, ContinuousQueryListener<K, C> listener);

   void removeContinuousQueryListener(ContinuousQueryListener<K, ?> listener);

   List<ContinuousQueryListener<K, ?>> getListeners();

   void removeAllListeners();
}
