package org.infinispan.hotrod.impl.cache;

/**
 * Besides the value, also contains a version and expiration information. Time values are server
 * time representations as returned by {@link org.infinispan.commons.time.TimeService#wallClockTime}
 *
 * @since 14.0
 */
public interface MetadataValue<V> extends VersionedValue<V>, Metadata {

}
