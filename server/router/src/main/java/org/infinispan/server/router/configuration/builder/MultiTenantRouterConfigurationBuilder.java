package org.infinispan.server.router.configuration.builder;

import org.infinispan.server.router.configuration.MultiTenantRouterConfiguration;

/**
 * Multi tenant router configuration builder.
 *
 * @author Sebastian Łaskawiec
 */
public class MultiTenantRouterConfigurationBuilder implements MultiTenantConfigurationBuilderParent {

    private RoutingBuilder routingBuilder = new RoutingBuilder(this);
    private HotRodRouterBuilder hotRodRouterBuilder = new HotRodRouterBuilder(this);
    private RestRouterBuilder restRouterBuilder = new RestRouterBuilder(this);

    @Override
    public RoutingBuilder routing() {
        return routingBuilder;
    }

    @Override
    public HotRodRouterBuilder hotrod() {
        hotRodRouterBuilder.enabled(true);
        return hotRodRouterBuilder;
    }

    @Override
    public RestRouterBuilder rest() {
        restRouterBuilder.enabled(true);
        return restRouterBuilder;
    }

    /**
     * Returns assembled configuration.
     */
    public MultiTenantRouterConfiguration build() {
        return new MultiTenantRouterConfiguration(routingBuilder.build(), hotRodRouterBuilder.build(), restRouterBuilder.build());
    }
}
