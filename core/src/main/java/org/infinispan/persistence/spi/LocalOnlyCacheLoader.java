package org.infinispan.persistence.spi;

/**
 * Marker interface for cache loaders that should only load values on the originating nodes.
 * An example of a loader that uses this interface is {@link org.infinispan.persistence.cluster.ClusterLoader}.
 *
 * @author Dan Berindei
 * @since 7.0
 */
public interface LocalOnlyCacheLoader {
}
