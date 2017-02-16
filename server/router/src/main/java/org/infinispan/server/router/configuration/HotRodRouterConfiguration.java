package org.infinispan.server.router.configuration;

import java.net.InetAddress;

/**
 * {@link org.infinispan.server.router.MultiTenantRouter}'s configuration for Hot Rod.
 */
public class HotRodRouterConfiguration extends AbstractRouterConfiguration {

    private final int sendBufferSize;
    private final int receiveBufferSize;
    private final boolean keepAlive;
    private final boolean tcpNoDelay;

    /**
     * Creates new configuration based on the IP address and port.
     *
     * @param ip                The IP address used for binding. Can not be <code>null</code>.
     * @param port              Port used for binding. Can be 0, in that case a random port is assigned.
     * @param keepAlive         Keep alive TCP setting.
     * @param receiveBufferSize Receive buffer size.
     * @param sendBufferSize    Send buffer size
     * @param tcpNoDelay        TCP No Delay setting.
     */
    public HotRodRouterConfiguration(InetAddress ip, int port, int sendBufferSize, int receiveBufferSize, boolean keepAlive, boolean tcpNoDelay) {
        super(ip, port);
        this.sendBufferSize = sendBufferSize;
        this.receiveBufferSize = receiveBufferSize;
        this.keepAlive = keepAlive;
        this.tcpNoDelay = tcpNoDelay;
    }

    /**
     * Returns TCP No Delay setting.
     */
    public boolean tcpNoDelay() {
        return tcpNoDelay;
    }

    /**
     * Returns TCP Keep Alive setting.
     */
    public boolean keepAlive() {
        return keepAlive;
    }

    /**
     * Returns Send buffer size.
     */
    public int sendBufferSize() {
        return sendBufferSize;
    }

    /**
     * Returns Receive buffer size.
     */
    public int receiveBufferSize() {
        return receiveBufferSize;
    }
}
