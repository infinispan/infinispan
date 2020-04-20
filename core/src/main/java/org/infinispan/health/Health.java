package org.infinispan.health;

import java.util.List;
import java.util.Set;

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
     * Returns per cache health for the provided cache names.
     *
     * @param cacheNames
     */
    List<CacheHealth> getCacheHealth(Set<String> cacheNames);

    /**
     * Gets basic information about the host.
     */
    HostInfo getHostInfo();
}
