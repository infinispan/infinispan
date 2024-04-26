package org.infinispan.server.router.router.impl.hotrod.handlers;

import java.util.List;
import java.util.Optional;

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
import io.netty.util.Mapping;

/**
 * Handler responsible for routing requests to proper backend based on SNI Host Name.
 *
 * @author Sebastian Łaskawiec
 */
public class SniRouteHandler extends SniHandler {
    private final RoutingTable routingTable;

    /**
     * Creates new {@link SniRouteHandler} based on SNI Domain mapping and the {@link RoutingTable}.
     *
     * @param mapping      SNI Host Name mapping.
     * @param routingTable The {@link RoutingTable} for supplying the {@link Route}s.
     */
    public SniRouteHandler(Mapping<String, SslContext> mapping, RoutingTable routingTable) {
        super(mapping);
        this.routingTable = routingTable;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        super.decode(ctx, in, out);

        if (isHandShaked()) {
            // At this point Netty has replaced SNIHandler (formally this) with SSLHandler in the pipeline.
            // Now we need to add other handlers at the tail of the queue
            RouterLogger.SERVER.debugf("Handshaked with hostname %s", hostname());
            Optional<Route<SniRouteSource, HotRodServerRouteDestination>> route = routingTable.streamRoutes(SniRouteSource.class, HotRodServerRouteDestination.class)
                    .filter(r -> r.getRouteSource().getSniHostName().equals(this.hostname()))
                    .findAny();

            HotRodServerRouteDestination routeDestination = route.orElseThrow(() -> RouterLogger.SERVER.noRouteFound()).getRouteDestination();
            ChannelInitializer<Channel> channelInitializer = routeDestination.getProtocolServer().getInitializer();

            ctx.pipeline().addLast(channelInitializer);
            RouterLogger.SERVER.debug("Replaced with route destination's handlers");
        }
    }

    /**
     * Return <code>true</code> if handshake was successful.
     */
    public boolean isHandShaked() {
        return this.hostname() != null;
    }
}
