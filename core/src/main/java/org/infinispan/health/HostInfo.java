package org.infinispan.health;

/**
 * Information about the host.
 *
 * @author Sebastian Łaskawiec
 * @since 9.0
 */
public interface HostInfo {

    /**
     * Returns the number of CPUs installed in the host.
     */
    int getNumberOfCpus();

    /**
     * Gets total memory in KB.
     */
    long getTotalMemoryKb();

    /**
     * Gets free memory in KB.
     */
    long getFreeMemoryInKb();
}
