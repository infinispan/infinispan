package org.infinispan.server.router.configuration;

import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.router.Router;

/**
 * {@link Router}'s configuration for Single Port.
 *
 * @author Sebastian Łaskawiec
 */
public class SinglePortRouterConfiguration extends ProtocolServerConfiguration {

    /**
     * Creates new configuration based on the IP address and port.
     *  @param ip                The IP address used for binding. Can not be <code>null</code>.
     * @param port              Port used for binding. Can be 0, in that case a random port is assigned.
     * @param sendBufferSize    Send buffer size
     * @param receiveBufferSize Receive buffer size.
     */
    public SinglePortRouterConfiguration(AttributeSet attributes, SslConfiguration ssl) {
        super(attributes, ssl);
    }

}
