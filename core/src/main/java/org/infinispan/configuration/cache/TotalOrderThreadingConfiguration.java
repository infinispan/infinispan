package org.infinispan.configuration.cache;

/**
 * Thread pool configuration for total order protocol
 *
 * Date: 1/17/12
 * Time: 11:43 AM
 *
 * @author pruivo
 */
public class TotalOrderThreadingConfiguration {

    private final int corePoolSize;// = 1;

    private final int maximumPoolSize;// = 8;

    private final long keepAliveTime;// = 1000; //milliseconds

    public TotalOrderThreadingConfiguration(int corePoolSize, int maximumPoolSize, long keepAliveTime) {
        this.corePoolSize = corePoolSize;
        this.maximumPoolSize = maximumPoolSize;
        this.keepAliveTime = keepAliveTime;
    }

    public int getCorePoolSize() {
        return corePoolSize;
    }

    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    public long getKeepAliveTime() {
        return keepAliveTime;
    }
}
