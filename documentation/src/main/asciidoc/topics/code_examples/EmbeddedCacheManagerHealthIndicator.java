package org.infinispan.sample;

import org.infinispan.health.ClusterHealth;
import org.infinispan.health.HealthStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.stereotype.Component;

@Component("infinispan")
public class EmbeddedCacheManagerHealthIndicator extends AbstractHealthIndicator {

    private final EmbeddedCacheManager cacheManager;

    public EmbeddedCacheManagerHealthIndicator(EmbeddedCacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }

    @Override
    protected void doHealthCheck(org.springframework.boot.actuate.health.Health.Builder builder) throws Exception {
        ClusterHealth clusterHealth = cacheManager.getHealth().getClusterHealth();
        HealthStatus status = clusterHealth.getHealthStatus();
        if (status == HealthStatus.HEALTHY || status == HealthStatus.HEALTHY_REBALANCING) {
            builder.up()
                   .withDetail("health", status.toString())
                   .withDetail("clusterName", clusterHealth.getClusterName())
                   .withDetail("nodes", cacheManager.getMembers().size());
        } else {
            builder.down()
                   .withDetail("health", status.toString())
                   .withDetail("clusterName", clusterHealth.getClusterName());
        }
    }
}
