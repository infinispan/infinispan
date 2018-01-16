package org.infinispan.server.router.configuration;

import org.infinispan.server.router.Router;
import org.infinispan.server.router.RoutingTable;

/**
 * Global {@link Router}'s configuration.
 *
 * @author Sebastian Łaskawiec
 */
public class RouterConfiguration {

    private final RoutingTable routingTable;
    private final HotRodRouterConfiguration hotRodRouterConfiguration;
    private final RestRouterConfiguration restRouterConfiguration;
    private final SinglePortRouterConfiguration singlePortRouterConfiguration;

    /**
     * Creates new configuration based on protocol configurations and the {@link RoutingTable}.
     *  @param routingTable              The {@link RoutingTable} for supplying {@link org.infinispan.server.router.routes.Route}s.
     * @param hotRodRouterConfiguration Hot Rod Configuration.
     * @param restRouterConfiguration   REST Configuration.
     * @param singlePortRouterConfiguration
     */
    public RouterConfiguration(RoutingTable routingTable, HotRodRouterConfiguration hotRodRouterConfiguration, RestRouterConfiguration restRouterConfiguration, SinglePortRouterConfiguration singlePortRouterConfiguration) {
        this.routingTable = routingTable;
        this.hotRodRouterConfiguration = hotRodRouterConfiguration;
        this.restRouterConfiguration = restRouterConfiguration;
        this.singlePortRouterConfiguration = singlePortRouterConfiguration;
    }

    /**
     * Gets the {@link RoutingTable}.
     */
    public RoutingTable getRoutingTable() {
        return routingTable;
    }

    /**
     * Gets Hot Rod Configuration.
     */
    public HotRodRouterConfiguration getHotRodRouterConfiguration() {
        return hotRodRouterConfiguration;
    }

    /**
     * Gets REST Configuration.
     */
    public RestRouterConfiguration getRestRouterConfiguration() {
        return restRouterConfiguration;
    }

    /**
     * Gets Single Port Configuration.
     */
    public SinglePortRouterConfiguration getSinglePortRouterConfiguration() {
        return singlePortRouterConfiguration;
    }
}
