package org.infinispan.server.router.configuration;

import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.core.configuration.IpFilterConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.router.Router;
import org.infinispan.server.router.router.impl.singleport.SinglePortEndpointRouter;

/**
 * {@link Router}'s configuration for Single Port.
 *
 * @author Sebastian Łaskawiec
 */
@ConfigurationFor(SinglePortEndpointRouter.class)
public class SinglePortRouterConfiguration extends ProtocolServerConfiguration {

    public static AttributeSet attributeDefinitionSet() {
        return new AttributeSet(SinglePortRouterConfiguration.class, ProtocolServerConfiguration.attributeDefinitionSet());
    }

    /**
     * Creates new configuration based on the IP address and port.
     */
    public SinglePortRouterConfiguration(AttributeSet attributes, SslConfiguration ssl, IpFilterConfiguration ipRules) {
        super("endpoint", attributes, ssl, ipRules);
    }
}
