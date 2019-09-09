package org.infinispan.health;

import java.util.List;

/**
 * An entry point for checking health status.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public interface Health {

    /**
     * Returns Cluster health.
     */
    ClusterHealth getClusterHealth();

    /**
     * Returns per cache health.
     */
    List<CacheHealth> getCacheHealth();

    /**
     * Gets basic information about the host.
     */
    HostInfo getHostInfo();
}
