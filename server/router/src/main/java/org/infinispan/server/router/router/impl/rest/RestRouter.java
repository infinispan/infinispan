package org.infinispan.server.router.router.impl.rest;

import java.lang.invoke.MethodHandles;
import java.net.InetAddress;
import java.util.Optional;

import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseFilter;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.rest.Server;
import org.infinispan.rest.embedded.netty4.NettyJaxrsServer;
import org.infinispan.rest.logging.RestAccessLoggingHandler;
import org.infinispan.server.router.RoutingTable;
import org.infinispan.server.router.configuration.RestRouterConfiguration;
import org.infinispan.server.router.logging.RouterLogger;
import org.infinispan.server.router.router.Router;
import org.infinispan.server.router.routes.PrefixedRouteSource;
import org.infinispan.server.router.routes.rest.NettyRestServerRouteDestination;
import org.jboss.resteasy.spi.ResteasyDeployment;

/**
 * {@link Router} implementation for REST. Uses {@link NettyJaxrsServer} internally.
 *
 * @author Sebastian Łaskawiec
 */
public class RestRouter implements Router {

    private static final RouterLogger logger = LogFactory.getLog(MethodHandles.lookup().lookupClass(), RouterLogger.class);

    private static final String REST_PREFIX = "rest/";
    private final RestRouterConfiguration configuration;
    private Optional<Integer> port = Optional.empty();
    private Optional<InetAddress> ip = Optional.empty();
    private Optional<NettyJaxrsServer> nettyServer = Optional.empty();

    public RestRouter(RestRouterConfiguration configuration) {
        this.configuration = configuration;
    }

    @Override
    public void start(RoutingTable routingTable) {
        try {
            NettyJaxrsServer netty = new NettyJaxrsServer();
            ResteasyDeployment deployment = new ResteasyDeployment();
            netty.setDeployment(deployment);
            nettyServer = Optional.of(netty);
            netty.setHostname(configuration.getIp().getHostName());
            netty.setPort(configuration.getPort());
            netty.setRootResourcePath("");
            netty.setSecurityDomain(null);
            netty.start();

            addDeployments(netty, routingTable);

            this.ip = Optional.of(configuration.getIp());
            this.port = Optional.of(configuration.getPort());

            logger.restRouterStarted(ip, port);
        } catch (Exception e) {
            throw logger.restRouterStartFailed(e);
        }
    }

    private void addDeployments(NettyJaxrsServer netty, RoutingTable routingTable) {
        routingTable.streamRoutes(PrefixedRouteSource.class, NettyRestServerRouteDestination.class)
                .forEach(r -> {
                    String routePrefix = r.getRouteSource().getRoutePrefix();
                    Server targetResource = r.getRouteDesitnation().getRestResource();
                    netty.getDeployment().getRegistry().addSingletonResource(targetResource, REST_PREFIX + routePrefix);
                    netty.getDeployment().getProviderFactory().register(new RestAccessLoggingHandler(), ContainerResponseFilter.class,
                            ContainerRequestFilter.class);
                });
    }

    @Override
    public void stop() {
        nettyServer.ifPresent(NettyJaxrsServer::stop);
        nettyServer = Optional.empty();
        ip = Optional.empty();
        port = Optional.empty();
    }

    @Override
    public Optional<InetAddress> getIp() {
        return ip;
    }

    @Override
    public Optional<Integer> getPort() {
        return port;
    }

    @Override
    public Protocol getProtocol() {
        return Protocol.REST;
    }
}
