package org.infinispan.query.continuous;

/**
 * @author anistor@redhat.com
 * @since 8.1
 */
public interface ContinuousQueryListener<K, V> {

   void resultJoining(K key, V value);

   void resultLeaving(K key);
}
