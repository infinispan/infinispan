/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.infinispan.server.endpoint.subsystem;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Optional;

import org.infinispan.rest.embedded.netty4.NettyRestServer;
import org.infinispan.rest.Server;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.router.MultiTenantRouter;
import org.infinispan.server.router.configuration.builder.HotRodRouterBuilder;
import org.infinispan.server.router.configuration.builder.MultiTenantRouterConfigurationBuilder;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.hotrod.NettyHandlerRouteDestination;
import org.infinispan.server.router.routes.hotrod.SniNettyRouteSource;
import org.infinispan.server.router.routes.rest.NettyRestServerRouteDestination;
import org.infinispan.server.router.routes.rest.RestRouteSource;
import org.jboss.as.domain.management.SecurityRealm;
import org.jboss.as.network.NetworkUtils;
import org.jboss.as.network.SocketBinding;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;

/**
 * Multi tenant router service
 *
 * @author Sebastian Łaskawiec
 */
class MultiTenantRouterService implements Service<MultiTenantRouter> {

    static class HotRodRouting {
        private final InjectedValue<HotRodServer> hotRod = new InjectedValue<>();
        private final InjectedValue<SecurityRealm> securityRealm = new InjectedValue<>();

        public InjectedValue<HotRodServer> getHotRod() {
            return hotRod;
        }

        public InjectedValue<SecurityRealm> getSecurityRealm() {
            return securityRealm;
        }
    }

    static class RestRouting {
        private final InjectedValue<NettyRestServer> rest = new InjectedValue<>();
        private final String name;

        public RestRouting(String name) {
            this.name = name;
        }

        public InjectedValue<NettyRestServer> getRest() {
            return rest;
        }

        public String getName() {
            return name;
        }
    }

    private static final String DEFAULT_NAME = "Multitenant Router";

    private final InjectedValue<SocketBinding> restSocketBinding = new InjectedValue<>();
    private final InjectedValue<SocketBinding> hotrodSocketBinding = new InjectedValue<>();
    private final java.util.Map<String, HotRodRouting> hotrodRouting = new HashMap<>();
    private final java.util.Map<String, RestRouting> restRouting = new HashMap<>();

    private final String name;
    private final MultiTenantRouterConfigurationBuilder configurationBuilder;
    private MultiTenantRouter router;

    MultiTenantRouterService(MultiTenantRouterConfigurationBuilder configurationBuilder, Optional<String> serverName) {
        this.name = constructServerName(serverName);
        this.configurationBuilder = configurationBuilder;
    }

    private final String constructServerName(Optional<String> name) {
        return name.orElse(DEFAULT_NAME);
    }

    @Override
    public synchronized void start(final StartContext context) throws StartException {
        ROOT_LOGGER.endpointStarting(name);
        try {
            SocketBinding hotrodSocketBinding = getHotrodSocketBinding().getOptionalValue();
            InetSocketAddress hotrodAddress = hotrodSocketBinding != null ? hotrodSocketBinding.getSocketAddress() : null;

            SocketBinding restSocketBinding = getRestSocketBinding().getOptionalValue();
            InetSocketAddress restAddress = restSocketBinding != null ? restSocketBinding.getSocketAddress() : null;

            if (hotrodAddress != null) {
                HotRodRouterBuilder hotrodConfigurationBuilder = configurationBuilder.hotrod();
                hotrodConfigurationBuilder.ip(hotrodAddress.getAddress());
                hotrodConfigurationBuilder.port(hotrodAddress.getPort());
            }
            if (restAddress != null) {
                configurationBuilder.rest().ip(restAddress.getAddress());
                configurationBuilder.rest().port(restAddress.getPort());
            }

            hotrodRouting.forEach((sniHostName, routing) -> {
                HotRodServer hotRod = routing.getHotRod().getValue();
                SecurityRealm securityRealm = routing.getSecurityRealm().getValue();

                SniNettyRouteSource source = new SniNettyRouteSource(sniHostName, securityRealm.getSSLContext());
                NettyHandlerRouteDestination destination = new NettyHandlerRouteDestination(hotRod.getQualifiedName(), hotRod.getInitializer());

                configurationBuilder.routing().add(new Route<>(source, destination));
            });

            restRouting.forEach((path, routing) -> {
                Server restResource = routing.getRest().getValue().getServer();
                String name = routing.getName();

                RestRouteSource source = new RestRouteSource(path);
                NettyRestServerRouteDestination destination = new NettyRestServerRouteDestination(name, restResource);

                configurationBuilder.routing().add(new Route<>(source, destination));
            });

            this.router = new MultiTenantRouter(configurationBuilder.build());
            this.router.start();

            ROOT_LOGGER.routerStarted(NetworkUtils.formatAddress(hotrodAddress), NetworkUtils.formatAddress(restAddress));
        } catch (Exception e) {
            throw ROOT_LOGGER.failedStart(e, name);
        }
    }

    @Override
    public synchronized void stop(final StopContext context) {
        if (router != null) {
            router.stop();
        }
    }

    @Override
    public synchronized MultiTenantRouter getValue() throws IllegalStateException {
        if (router == null) {
            throw ROOT_LOGGER.serviceNotStarted();
        }
        return router;
    }

    InjectedValue<SocketBinding> getHotrodSocketBinding() {
        return hotrodSocketBinding;
    }

    HotRodRouting getHotRodRouting(String sniHostName) {
        return hotrodRouting.computeIfAbsent(sniHostName, v -> new HotRodRouting());
    }

    InjectedValue<SocketBinding> getRestSocketBinding() {
        return restSocketBinding;
    }

    RestRouting getRestRouting(String path, String name) {
        return restRouting.computeIfAbsent(path, v -> new RestRouting(name));
    }

    public void tcpNoDelay(boolean tcpNoDelay) {
        this.configurationBuilder.hotrod().tcpNoDelay(tcpNoDelay);
    }

    public void keepAlive(boolean keepAlive) {
        this.configurationBuilder.hotrod().keepAlive(keepAlive);
    }

    public void sendBufferSize(int sendBufferSize) {
        this.configurationBuilder.hotrod().sendBufferSize(sendBufferSize);
    }

    public void receiveBufferSize(int receiveBufferSize) {
        this.configurationBuilder.hotrod().receiveBufferSize(receiveBufferSize);
    }
}
