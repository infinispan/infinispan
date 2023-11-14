package org.infinispan.server.router.configuration.builder;

import org.infinispan.server.router.configuration.RouterConfiguration;

/**
 * Multi tenant router configuration builder.
 *
 * @author Sebastian Łaskawiec
 */
public class RouterConfigurationBuilder implements ConfigurationBuilderParent {

    private final RoutingBuilder routingBuilder = new RoutingBuilder(this);
    private final HotRodRouterBuilder hotRodRouterBuilder = new HotRodRouterBuilder(this);
    private final RestRouterBuilder restRouterBuilder = new RestRouterBuilder(this);
    private final SinglePortRouterBuilder singlePortRouterBuilder = new SinglePortRouterBuilder(this);

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

    @Override
    public SinglePortRouterBuilder singlePort() {
        singlePortRouterBuilder.enabled(true);
        return singlePortRouterBuilder;
    }

    /**
     * Returns assembled configuration.
     */
    public RouterConfiguration build() {
        return new RouterConfiguration(routingBuilder.build(), hotRodRouterBuilder.build(), restRouterBuilder.build(), singlePortRouterBuilder.build());
    }
}
