package org.infinispan.query.continuous;

/**
 * @author anistor@redhat.com
 * @since 8.0
 * @deprecated replaced by {@link org.infinispan.query.api.continuous.ContinuousQueryListener}; to be removed in 8.3
 */
public interface ContinuousQueryResultListener<K, V> extends ContinuousQueryListener<K, V> {
}
