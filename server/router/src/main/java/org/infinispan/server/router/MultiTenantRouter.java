package org.infinispan.server.router;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.router.configuration.MultiTenantRouterConfiguration;
import org.infinispan.server.router.logging.RouterLogger;
import org.infinispan.server.router.router.Router;
import org.infinispan.server.router.router.impl.hotrod.HotRodRouter;
import org.infinispan.server.router.router.impl.rest.RestRouter;

/**
 * The main entry point for the router.
 *
 * @author Sebastian Łaskawiec
 */
public class MultiTenantRouter {

    private static final RouterLogger logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), RouterLogger.class);

    private final MultiTenantRouterConfiguration routerConfiguration;
    private final Set<Router> routers = new HashSet<>();

    /**
     * Creates new {@link MultiTenantRouter} based on {@link MultiTenantRouterConfiguration}.
     *
     * @param routerConfiguration {@link MultiTenantRouterConfiguration} object.
     */
    public MultiTenantRouter(MultiTenantRouterConfiguration routerConfiguration) {
        this.routerConfiguration = routerConfiguration;
        if(routerConfiguration.getHotRodRouterConfiguration() != null) {
            routers.add(new HotRodRouter(routerConfiguration.getHotRodRouterConfiguration()));
        }
        if(routerConfiguration.getRestRouterConfiguration() != null) {
            routers.add(new RestRouter(routerConfiguration.getRestRouterConfiguration()));
        }
    }

    /**
     * Starts the router.
     */
    public void start() {
        routers.forEach(r -> r.start(routerConfiguration.getRoutingTable()));
        logger.printOutRoutingTable(routerConfiguration.getRoutingTable());
    }

    /**
     * Stops the router.
     */
    public void stop() {
        routers.forEach(r -> r.stop());
    }

    /**
     * Gets internal {@link Router} implementation for given protocol.
     *
     * @param protocol Protocol for obtaining the router.
     * @return The {@link Router} implementation.
     */
    public Optional<Router> getRouter(Router.Protocol protocol) {
        return routers.stream().filter(r -> r.getProtocol() == protocol).findFirst();
    }
}
