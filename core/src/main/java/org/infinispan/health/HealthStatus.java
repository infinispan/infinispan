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
   DEGRADED,

   /**
    * Given entity is healthy.
    */
   HEALTHY,

   /**
    * The given entity is still initializing.
    *
    * <p>This can happen when the entity does not have the time to completely initialize or when it is recovering after a cluster shutdown.</p>
    */
   INITIALIZING,

   /**
    * Given entity is healthy but a rebalance is in progress.
    */
   HEALTHY_REBALANCING,

   /**
    * The cache did not start due to a error.
    */
   FAILED
}
