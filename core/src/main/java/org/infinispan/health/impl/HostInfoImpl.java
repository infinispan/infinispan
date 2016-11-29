package org.infinispan.health.impl;

import org.infinispan.health.HostInfo;

public class HostInfoImpl implements HostInfo {

    @Override
    public int getNumberOfCpus() {
        return Runtime.getRuntime().availableProcessors();
    }

    @Override
    public long getTotalMemoryKb() {
        return Runtime.getRuntime().totalMemory() / 1024;
    }

    @Override
    public long getFreeMemoryInKb() {
        return Runtime.getRuntime().freeMemory() / 1024;
    }
}
