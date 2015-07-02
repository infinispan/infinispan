package org.infinispan.query.continuous;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
public interface ContinuousQueryResultListener<K, V> {

   void resultJoining(K key, V value);

   void resultLeaving(K key);
}
