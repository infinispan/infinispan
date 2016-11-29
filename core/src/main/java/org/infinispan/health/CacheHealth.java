package org.infinispan.health;

/**
 * Cache health information.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public interface CacheHealth {

    /**
     * Returns Cache name.
     */
    String getCacheName();

    /**
     * Returns Cache health status.
     */
    HealthStatus getStatus();
}
