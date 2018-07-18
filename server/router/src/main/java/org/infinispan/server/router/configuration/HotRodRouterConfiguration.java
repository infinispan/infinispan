package org.infinispan.server.router.configuration;

import java.net.InetAddress;

import org.infinispan.server.router.Router;

/**
 * {@link Router}'s configuration for Hot Rod.
 *
 * @author Sebastian Łaskawiec
 */
public class HotRodRouterConfiguration extends AbstractRouterConfiguration {

    private final int sendBufferSize;
    private final int receiveBufferSize;
    private final boolean tcpKeepAlive;
    private final boolean tcpNoDelay;

    /**
     * Creates new configuration based on the IP address and port.
     *
     * @param ip                The IP address used for binding. Can not be <code>null</code>.
     * @param port              Port used for binding. Can be 0, in that case a random port is assigned.
     * @param tcpKeepAlive         Keep alive TCP setting.
     * @param receiveBufferSize Receive buffer size.
     * @param sendBufferSize    Send buffer size
     * @param tcpNoDelay        TCP No Delay setting.
     */
    public HotRodRouterConfiguration(InetAddress ip, int port, int sendBufferSize, int receiveBufferSize, boolean tcpKeepAlive, boolean tcpNoDelay) {
        super(ip, port);
        this.sendBufferSize = sendBufferSize;
        this.receiveBufferSize = receiveBufferSize;
        this.tcpKeepAlive = tcpKeepAlive;
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
    public boolean tcpKeepAlive() {
        return tcpKeepAlive;
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
