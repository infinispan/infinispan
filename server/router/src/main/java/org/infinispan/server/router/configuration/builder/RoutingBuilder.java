package org.infinispan.server.router.configuration.builder;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.router.RoutingTable;
import org.infinispan.server.router.configuration.RouterConfiguration;
import org.infinispan.server.router.logging.RouterLogger;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.RouteDestination;
import org.infinispan.server.router.routes.RouteSource;

/**
 * Builder for constructing a {@link RoutingTable}.
 *
 * @author Sebastian Łaskawiec
 */
public class RoutingBuilder implements ConfigurationBuilderParent {

    protected static final RouterLogger logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), RouterLogger.class);

    private final RouterConfigurationBuilder parent;
    private Set<Route<? extends RouteSource, ? extends RouteDestination>> routes = new HashSet<>();

    /**
     * Creates new {@link RoutingBuilder}.
     *
     * @param parent Parent {@link RouterConfiguration}.
     */
    public RoutingBuilder(RouterConfigurationBuilder parent) {
        this.parent = parent;
    }

    /**
     * Adds a {@link Route} to the {@link RoutingTable}.
     *
     * @param route         {@link Route} to be added.
     * @param <Source>      {@link RouteSource} type.
     * @param <Destination> {@link RouteDestination} type.
     * @return This builder.
     */
    public <Source extends RouteSource, Destination extends RouteDestination> RoutingBuilder add(Route<Source, Destination> route) {
        routes.add(route);
        return this;
    }

    protected RoutingTable build() {
        try {
            routes.forEach(r -> r.validate());
        } catch (Exception e) {
            throw logger.configurationValidationError(e);
        }
        return new RoutingTable(routes);
    }

    @Override
    public RoutingBuilder routing() {
        return parent.routing();
    }

    @Override
    public HotRodRouterBuilder hotrod() {
        return parent.hotrod();
    }

    @Override
    public RestRouterBuilder rest() {
        return parent.rest();
    }

    @Override
    public SinglePortRouterBuilder singlePort() {
        return parent.singlePort();
    }
}
