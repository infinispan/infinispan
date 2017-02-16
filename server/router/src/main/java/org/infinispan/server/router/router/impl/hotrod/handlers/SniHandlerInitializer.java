package org.infinispan.server.router.router.impl.hotrod.handlers;

import java.lang.invoke.MethodHandles;
import java.util.Optional;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.server.router.RoutingTable;
import org.infinispan.server.router.logging.RouterLogger;
import org.infinispan.server.router.router.impl.hotrod.handlers.util.SslUtils;
import org.infinispan.server.router.routes.RouteDestination;
import org.infinispan.server.router.routes.SniRouteSource;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.ssl.SslContext;
import io.netty.util.DomainMappingBuilder;
import io.netty.util.DomainNameMapping;

/**
 * Initializer for SNI Handlers.
 *
 * @author Sebastian Łaskawiec
 */
public class SniHandlerInitializer extends ChannelInitializer<Channel> {

    private static final RouterLogger logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), RouterLogger.class);

    private final RoutingTable routingTable;

    /**
     * Creates new {@link SniHandlerInitializer} based on the routing table.
     *
     * @param routingTable {@link RoutingTable} for supplying the {@link org.infinispan.server.router.routes.Route}s.
     */
    public SniHandlerInitializer(RoutingTable routingTable) {
        this.routingTable = routingTable;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
        SslContext defaultContext = SslUtils.INSTANCE.toNettySslContext(Optional.empty());
        DomainMappingBuilder<SslContext> domainMappingBuilder = new DomainMappingBuilder<>(defaultContext);

        routingTable.streamRoutes(SniRouteSource.class, RouteDestination.class)
                .map(r -> r.getRouteSource())
                .forEach(r -> domainMappingBuilder.add(r.getSniHostName(), SslUtils.INSTANCE.toNettySslContext(Optional.of(r.getSslContext()))));

        DomainNameMapping<SslContext> domainNameMapping = domainMappingBuilder.build();

        logger.initializedSni(domainNameMapping);

        channel.pipeline().addLast(new SniRouteHandler(domainNameMapping, routingTable));
    }
}
