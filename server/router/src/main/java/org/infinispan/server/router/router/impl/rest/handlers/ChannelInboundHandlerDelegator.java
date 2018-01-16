package org.infinispan.server.router.router.impl.rest.handlers;

import java.lang.invoke.MethodHandles;
import java.util.Optional;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.rest.Http11RequestHandler;
import org.infinispan.server.router.RoutingTable;
import org.infinispan.server.router.logging.RouterLogger;
import org.infinispan.server.router.routes.PrefixedRouteSource;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.rest.RestServerRouteDestination;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.FullHttpRequest;

/**
 * Handler responsible for dispatching requests into proper REST handlers.
 *
 * @author Sebastian Łaskawiec
 */
public class ChannelInboundHandlerDelegator extends SimpleChannelInboundHandler<FullHttpRequest> {

   private static final RouterLogger logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), RouterLogger.class);

   private final RoutingTable routingTable;

   public ChannelInboundHandlerDelegator(RoutingTable routingTable) {
      this.routingTable = routingTable;
   }

   @Override
   protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
      String[] uriSplitted = msg.uri().split("/");
      //we are paring something like this: /rest/<context or prefix>/...
      if (uriSplitted.length < 2) {
         throw logger.noRouteFound();
      }
      String context = uriSplitted[2];

      logger.debugf("Decoded context %s", context);
      Optional<Route<PrefixedRouteSource, RestServerRouteDestination>> route = routingTable.streamRoutes(PrefixedRouteSource.class, RestServerRouteDestination.class)
            .filter(r -> r.getRouteSource().getRoutePrefix().equals(context))
            .findAny();

      RestServerRouteDestination routeDestination = route.orElseThrow(() -> logger.noRouteFound()).getRouteDesitnation();
      Http11RequestHandler restHandler = (Http11RequestHandler) routeDestination.getRestServer().getRestChannelInitializer().getHttp11To2UpgradeHandler().getHttp1Handler();

      //before passing it to REST Handler, we need to replace path. The handler should not be aware of additional context
      //used for multi-tenant prefixes
      String uriWithoutMultiTenantPrefix = "";
      for (int i = 0; i < uriSplitted.length; ++i) {
         if (i == 1) {
            //this is the main uri prefix - "rest", we want to get rid of that.
            continue;
         }
         uriWithoutMultiTenantPrefix += uriSplitted[i];
         if (i < uriSplitted.length - 1) {
            uriWithoutMultiTenantPrefix += "/";
         }
      }
      msg.setUri(uriWithoutMultiTenantPrefix);

      restHandler.channelRead0(ctx, msg);
   }
}
