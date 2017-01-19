package org.infinispan.server.router.configuration;

import org.infinispan.server.router.RoutingTable;

/**
 * Global {@link org.infinispan.server.router.MultiTenantRouter}'s configuration.
 */
public class MultiTenantRouterConfiguration {

    private final RoutingTable routingTable;
    private final HotRodRouterConfiguration hotRodRouterConfiguration;
    private final RestRouterConfiguration restRouterConfiguration;

    /**
     * Creates new configuration based on protocol configurations and the {@link RoutingTable}.
     *
     * @param routingTable              The {@link RoutingTable} for supplying {@link org.infinispan.server.router.routes.Route}s.
     * @param hotRodRouterConfiguration Hot Rod Configuration.
     * @param restRouterConfiguration   REST Configuration.
     */
    public MultiTenantRouterConfiguration(RoutingTable routingTable, HotRodRouterConfiguration hotRodRouterConfiguration, RestRouterConfiguration restRouterConfiguration) {
        this.routingTable = routingTable;
        this.hotRodRouterConfiguration = hotRodRouterConfiguration;
        this.restRouterConfiguration = restRouterConfiguration;
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
}
