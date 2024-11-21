package org.infinispan.client.hotrod;

/**
 * Besides the value, also contains a version and expiration information. Time values are server
 * time representations as returned by {@link org.infinispan.commons.time.TimeService#wallClockTime}
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface MetadataValue<V> extends VersionedValue<V>, Metadata {

}
