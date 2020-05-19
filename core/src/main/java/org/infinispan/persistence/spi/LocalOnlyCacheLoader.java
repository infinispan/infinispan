package org.infinispan.persistence.spi;

/**
 * Marker interface for cache loaders that should only load values on the originating nodes.
 * An example of a loader that uses this interface is {@link org.infinispan.persistence.cluster.ClusterLoader}.
 *
 * @author Dan Berindei
 * @since 7.0
 * @deprecated since 11.0. To be removed in 14.0 ISPN-11864 with no direct replacement.
 */
@Deprecated
public interface LocalOnlyCacheLoader {
}
