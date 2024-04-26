package org.infinispan.server.router.router.impl.hotrod.handlers;

import java.util.Optional;

import org.infinispan.server.router.RoutingTable;
import org.infinispan.server.router.logging.RouterLogger;
import org.infinispan.server.router.router.impl.hotrod.handlers.util.SslUtils;
import org.infinispan.server.router.routes.RouteDestination;
import org.infinispan.server.router.routes.SniRouteSource;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.handler.ssl.SslContext;
import io.netty.util.DomainWildcardMappingBuilder;
import io.netty.util.Mapping;

/**
 * Initializer for SNI Handlers.
 *
 * @author Sebastian Łaskawiec
 */
public class SniHandlerInitializer extends ChannelInitializer<Channel> {
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
    protected void initChannel(Channel channel)  {
        SslContext defaultContext = SslUtils.INSTANCE.toNettySslContext(Optional.empty());
        DomainWildcardMappingBuilder<SslContext> domainMappingBuilder = new DomainWildcardMappingBuilder<>(defaultContext);

        routingTable.streamRoutes(SniRouteSource.class, RouteDestination.class)
                .map(r -> r.getRouteSource())
                .forEach(r -> domainMappingBuilder.add(r.getSniHostName(), SslUtils.INSTANCE.toNettySslContext(Optional.of(r.getSslContext()))));

        Mapping<String, SslContext> domainNameMapping = domainMappingBuilder.build();

        RouterLogger.SERVER.debugf("Using SNI Handler with domain mapping %s", domainNameMapping);

        channel.pipeline().addLast(new SniRouteHandler(domainNameMapping, routingTable));
    }
}
