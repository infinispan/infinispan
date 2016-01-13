package org.infinispan.client.hotrod.event;

/**
 * Listener for continuous query events.
 *
 * @author anistor@redhat.com
 * @since 8.0
 * @deprecated replaced by {@link org.infinispan.query.api.continuous.ContinuousQueryListener}; to be removed in 8.3
 */
public interface ContinuousQueryListener<K, V> extends org.infinispan.query.api.continuous.ContinuousQueryListener<K, V> {
}
