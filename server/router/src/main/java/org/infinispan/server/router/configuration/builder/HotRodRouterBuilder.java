package org.infinispan.server.router.configuration.builder;

import org.infinispan.server.router.configuration.HotRodRouterConfiguration;

/**
 * Configuration builder for Hot Rod.
 *
 * @author Sebastian Łaskawiec
 */
public class HotRodRouterBuilder extends AbstractRouterBuilder {

    private int sendBufferSize = 0;
    private int receiveBufferSize = 0;
    private boolean tcpKeepAlive = false;
    private boolean tcpNoDelay = true;

    /**
     * Creates new {@link HotRodRouterBuilder}.
     *
     * @param parent Parent {@link ConfigurationBuilderParent}
     */
    public HotRodRouterBuilder(ConfigurationBuilderParent parent) {
        super(parent);
    }

    /**
     * Builds {@link HotRodRouterConfiguration}.
     */
    public HotRodRouterConfiguration build() {
        if (this.enabled) {
            try {
                validate();
            } catch (Exception e) {
                throw logger.configurationValidationError(e);
            }
            return new HotRodRouterConfiguration(ip, port, sendBufferSize, receiveBufferSize, tcpKeepAlive, tcpNoDelay);
        }
        return null;
    }

    /**
     * Sets TCP No Delay.
     */
    public HotRodRouterBuilder tcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    /**
     * Sets TCP Keep Alive
     */
    public HotRodRouterBuilder tcpKeepAlive(boolean tcpKeepAlive) {
        this.tcpKeepAlive = tcpKeepAlive;
        return this;
    }

    /**
     * Sets Send buffer size
     *
     * @param sendBufferSize Send buffer size, must be greater than 0.
     */
    public HotRodRouterBuilder sendBufferSize(int sendBufferSize) {
        this.sendBufferSize = sendBufferSize;
        return this;
    }

    /**
     * Sets Receive buffer size.
     *
     * @param receiveBufferSize Receive buffer size, must be greater than 0.
     */
    public HotRodRouterBuilder receiveBufferSize(int receiveBufferSize) {
        this.receiveBufferSize = receiveBufferSize;
        return this;
    }

    @Override
    protected void validate() {
        super.validate();
        if (receiveBufferSize < 0) {
            throw new IllegalArgumentException("Receive buffer size can not be negative");
        }
        if (sendBufferSize < 0) {
            throw new IllegalArgumentException("Send buffer size can not be negative");
        }
    }
}
