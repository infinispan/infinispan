package org.infinispan.server.router.router.impl.hotrod.handlers;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Optional;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.router.RoutingTable;
import org.infinispan.server.router.logging.RouterLogger;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.SniRouteSource;
import org.infinispan.server.router.routes.hotrod.HotRodServerRouteDestination;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.ssl.SniHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.util.DomainNameMapping;

/**
 * Handler responsible for routing requests to proper backend based on SNI Host Name.
 *
 * @author Sebastian Łaskawiec
 */
public class SniRouteHandler extends SniHandler {

    private static final RouterLogger logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), RouterLogger.class);

    private final RoutingTable routingTable;

    /**
     * Creates new {@link SniRouteHandler} based on SNI Domain mapping and the {@link RoutingTable}.
     *
     * @param mapping      SNI Host Name mapping.
     * @param routingTable The {@link RoutingTable} for supplying the {@link Route}s.
     */
    public SniRouteHandler(DomainNameMapping<? extends SslContext> mapping, RoutingTable routingTable) {
        super(mapping);
        this.routingTable = routingTable;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        super.decode(ctx, in, out);

        if (isHandShaked()) {
            // At this point Netty has replaced SNIHandler (formally this) with SSLHandler in the pipeline.
            // Now we need to add other handlers at the tail of the queue
            logger.debugf("Handshaked with hostname %s", hostname());
            Optional<Route<SniRouteSource, HotRodServerRouteDestination>> route = routingTable.streamRoutes(SniRouteSource.class, HotRodServerRouteDestination.class)
                    .filter(r -> r.getRouteSource().getSniHostName().equals(this.hostname()))
                    .findAny();

            HotRodServerRouteDestination routeDestination = route.orElseThrow(() -> logger.noRouteFound()).getRouteDesitnation();
            ChannelInitializer<Channel> channelInitializer = routeDestination.getHotrodServer().getInitializer();

            ctx.pipeline().addLast(channelInitializer);
            logger.debug("Replaced with route destination's handlers");
        }
    }

    /**
     * Return <code>true</code> if handshake was successful.
     */
    public boolean isHandShaked() {
        return this.hostname() != null;
    }
}
