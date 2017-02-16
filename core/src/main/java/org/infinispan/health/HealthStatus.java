package org.infinispan.health;

/**
 * General Health status.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public enum HealthStatus {
    /**
     * Given entity is unhealthy.
     *
     * <p>
     *    An unhealthy status means that a cache is in {@link org.infinispan.partitionhandling.AvailabilityMode#DEGRADED_MODE}.
     *    Please keep in mind that in the future additional rules might be added to reflect Unhealthy status of the cache.
     * </p>.
     */
    UNHEALTHY,

    /**
     * Given entity is healthy.
     */
    HEALTHY,

    /**
     * Given entity is healthy but a rebalance is in progress.
     */
    REBALANCING
}
