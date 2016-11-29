package org.infinispan.health;

import java.util.List;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * An entry point for checking health status.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
@Scope(Scopes.GLOBAL)
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
