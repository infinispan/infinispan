package org.infinispan.query.api.continuous;

/**
 * A container of continuous query listeners for a cache.
 * <p>
 * Implementations are not expected to be threadsafe.
 *
 * @author anistor@redhat.com
 * @since 8.2
 */
public interface ContinuousQuery<K, V> extends org.infinispan.commons.api.query.ContinuousQuery<K, V> {

}
