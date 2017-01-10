package org.infinispan.health.impl.jmx;

import java.util.List;

import org.infinispan.health.CacheHealth;
import org.infinispan.health.Health;
import org.infinispan.health.jmx.HealthJMXExposer;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;

/**
 * A JMX exposer (or adapter) for Health API.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
@MBean(objectName = HealthJMXExposer.OBJECT_NAME, description = "Health Check API")
public class HealthJMXExposerImpl implements HealthJMXExposer {

    private final Health health;

    public HealthJMXExposerImpl(Health health) {
        this.health = health;
    }

    @ManagedAttribute(displayName = "Number of CPUs in the host", description = "Number of CPUs in the host")
    @Override
    public int getNumberOfCpus() {
        return health.getHostInfo().getNumberOfCpus();
    }

    @ManagedAttribute(displayName = "The amount of total memory (KB) in the host", description = "The amount of total memory (KB) in the host")
    @Override
    public long getTotalMemoryKb() {
        return health.getHostInfo().getTotalMemoryKb();
    }

    @ManagedAttribute(displayName = "The amount of free memory (KB) in the host", description = "The amount of free memory (KB) in the host")
    @Override
    public long getFreeMemoryKb() {
        return health.getHostInfo().getFreeMemoryInKb();
    }

    @ManagedAttribute(displayName = "Cluster health status", description = "Cluster health status")
    @Override
    public String getClusterHealth() {
        return health.getClusterHealth().getHealthStatus().toString();
    }

    @ManagedAttribute(displayName = "Cluster name", description = "Cluster name")
    @Override
    public String getClusterName() {
        return health.getClusterHealth().getClusterName();
    }

    @ManagedAttribute(displayName = "Total nodes in the cluster", description = "Total nodes in the cluster")
    @Override
    public int getNumberOfNodes() {
        return health.getClusterHealth().getNumberOfNodes();
    }

    @ManagedAttribute(displayName = "Per Cache statuses", description = "Per Cache statuses")
    @Override
    public String[] getCacheHealth() {
        List<CacheHealth> cacheHealths = health.getCacheHealth();
        String[] returnValues = new String[cacheHealths.size() * 2];
        for (int i = 0; i < cacheHealths.size(); ++i) {
            returnValues[i * 2] = cacheHealths.get(i).getCacheName();
            returnValues[i * 2 + 1] = cacheHealths.get(i).getStatus().toString();
        }
        return returnValues;
    }
}
