package org.infinispan.health.jmx;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

/**
 * A Contract for exposing Health API over the JMX.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
@Scope(Scopes.GLOBAL)
public interface HealthJMXExposer {

    /**
     * JMX Object name.
     */
    String OBJECT_NAME = "CacheContainerHealth";

    /**
     * Returns the total amount of CPUs for the JVM.
     */
    int getNumberOfCpus();

    /**
     * Returns the amount of total memory (KB) in the host.
     */
    long getTotalMemoryKb();

    /**
     * Returns the amount of free memory (KB) in the host.
     */
    long getFreeMemoryKb();

    /**
     * Returns cluster health status.
     */
    String getClusterHealth();

    /**
     * Returns cluster name.
     */
    String getClusterName();

    /**
     * Returns total nodes in the cluster.
     */
    int getNumberOfNodes();

    /**
     * Returns per Cache statuses.
     */
    String[] getCacheHealth();
}
