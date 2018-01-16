package org.infinispan.server.router;

import java.lang.invoke.MethodHandles;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.router.configuration.RouterConfiguration;
import org.infinispan.server.router.logging.RouterLogger;
import org.infinispan.server.router.router.EndpointRouter;
import org.infinispan.server.router.router.impl.hotrod.HotRodEndpointRouter;
import org.infinispan.server.router.router.impl.rest.RestEndpointRouter;
import org.infinispan.server.router.router.impl.singleport.SinglePortEndpointRouter;

/**
 * The main entry point for the router.
 *
 * @author Sebastian Łaskawiec
 */
public class Router {

   private static final RouterLogger logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), RouterLogger.class);

   private final RouterConfiguration routerConfiguration;
   private final Set<EndpointRouter> endpointRouters = new HashSet<>();

   /**
    * Creates new {@link Router} based on {@link RouterConfiguration}.
    *
    * @param routerConfiguration {@link RouterConfiguration} object.
    */
   public Router(RouterConfiguration routerConfiguration) {
      this.routerConfiguration = routerConfiguration;
      if (routerConfiguration.getHotRodRouterConfiguration() != null) {
         endpointRouters.add(new HotRodEndpointRouter(routerConfiguration.getHotRodRouterConfiguration()));
      }
      if (routerConfiguration.getRestRouterConfiguration() != null) {
         endpointRouters.add(new RestEndpointRouter(routerConfiguration.getRestRouterConfiguration()));
      }
      if (routerConfiguration.getSinglePortRouterConfiguration() != null) {
         endpointRouters.add(new SinglePortEndpointRouter(routerConfiguration.getSinglePortRouterConfiguration()));
      }
   }

   /**
    * Starts the router.
    */
   public void start() {
      endpointRouters.forEach(r -> r.start(routerConfiguration.getRoutingTable()));
      logger.printOutRoutingTable(routerConfiguration.getRoutingTable());
   }

   /**
    * Stops the router.
    */
   public void stop() {
      endpointRouters.forEach(r -> r.stop());
   }

   /**
    * Gets internal {@link EndpointRouter} implementation for given protocol.
    *
    * @param protocol Protocol for obtaining the router.
    * @return The {@link EndpointRouter} implementation.
    */
   public Optional<EndpointRouter> getRouter(EndpointRouter.Protocol protocol) {
      return endpointRouters.stream().filter(r -> r.getProtocol() == protocol).findFirst();
   }
}
