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

import java.lang.invoke.MethodHandles;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Optional;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Features;
import org.infinispan.rest.RestServer;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.router.Router;
import org.infinispan.server.router.configuration.builder.RouterConfigurationBuilder;
import org.infinispan.server.router.routes.Route;
import org.infinispan.server.router.routes.hotrod.HotRodServerRouteDestination;
import org.infinispan.server.router.routes.hotrod.SniNettyRouteSource;
import org.infinispan.server.router.routes.rest.RestRouteSource;
import org.infinispan.server.router.routes.rest.RestServerRouteDestination;
import org.infinispan.server.router.routes.singleport.SinglePortRouteSource;
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
class RouterService implements Service<Router> {

    public static final String SINGLE_PORT_FEATURE = "single-port";

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
        private final InjectedValue<RestServer> rest = new InjectedValue<>();
        private final String name;

        public RestRouting(String name) {
            this.name = name;
        }

        public InjectedValue<RestServer> getRest() {
            return rest;
        }

        public String getName() {
            return name;
        }
    }

    static class SinglePortRouting {
        private final InjectedValue<RestServer> restServer = new InjectedValue<>();
        private final InjectedValue<HotRodServer> hotrodServer = new InjectedValue<>();
        private final InjectedValue<SecurityRealm> securityRealm = new InjectedValue<>();

        public InjectedValue<HotRodServer> getHotrodServer() {
            return hotrodServer;
        }

        public InjectedValue<RestServer> getRestServer() {
            return restServer;
        }

        public InjectedValue<SecurityRealm> getSecurityRealm() {
            return securityRealm;
        }
    }

    private static final String DEFAULT_NAME = "EndpointRouter";

    private final InjectedValue<SocketBinding> restSocketBinding = new InjectedValue<>();
    private final InjectedValue<SocketBinding> hotrodSocketBinding = new InjectedValue<>();
    private final InjectedValue<SocketBinding> singlePortSocketBinding = new InjectedValue<>();
    private final java.util.Map<String, HotRodRouting> hotrodRouting = new HashMap<>();
    private final java.util.Map<String, RestRouting> restRouting = new HashMap<>();
    private final SinglePortRouting singlePortRouting = new SinglePortRouting();

    private final String name;
    private final RouterConfigurationBuilder configurationBuilder;
    private Router router;

    RouterService(RouterConfigurationBuilder configurationBuilder, Optional<String> serverName) {
        this.name = constructServerName(serverName);
        this.configurationBuilder = configurationBuilder;
    }

    private final String constructServerName(Optional<String> name) {
        return name.orElse(DEFAULT_NAME);
    }

   /**
    * Verify that the single port feature is enabled. Initializes features if the provided one is null, allowing for
    * lazily initialized Features instance.
    * @param features existing instance or null
    * @return the features instance that was checked, always non null
    * @throws StartException thrown if the single port feature was disabled
    */
    private Features checkSinglePortEnabled(Features features) throws StartException {
        if (features == null) {
            features = new Features(getClass().getClassLoader());
        }
        if (!features.isAvailable(SINGLE_PORT_FEATURE)) {
            throw LogFactory.getLog(MethodHandles.lookup().lookupClass()).featureDisabled(SINGLE_PORT_FEATURE);
        }
        return features;
    }

    @Override
    public synchronized void start(final StartContext context) throws StartException {
        ROOT_LOGGER.endpointStarting(name);
        try {
            SocketBinding hotrodSocketBinding = getHotrodSocketBinding().getOptionalValue();
            InetSocketAddress hotrodAddress = hotrodSocketBinding != null ? hotrodSocketBinding.getSocketAddress() : null;

            SocketBinding restSocketBinding = getRestSocketBinding().getOptionalValue();
            InetSocketAddress restAddress = restSocketBinding != null ? restSocketBinding.getSocketAddress() : null;

            SocketBinding singlePortSocketBinding = getSinglePortSocketBinding().getOptionalValue();
            InetSocketAddress singlePortAddress = singlePortSocketBinding != null ? singlePortSocketBinding.getSocketAddress() : null;

            if (hotrodAddress != null) {
                configurationBuilder.hotrod().ip(hotrodAddress.getAddress());
                configurationBuilder.hotrod().port(hotrodAddress.getPort());
            }
            if (restAddress != null) {
                configurationBuilder.rest().ip(restAddress.getAddress());
                configurationBuilder.rest().port(restAddress.getPort());
            }
            // Only initialize features if we have a valid feature configured - such as single port
            Features features = null;
            if (singlePortAddress != null) {
                features = checkSinglePortEnabled(features);
                configurationBuilder.singlePort().ip(singlePortAddress.getAddress());
                configurationBuilder.singlePort().port(singlePortAddress.getPort());
            }

            hotrodRouting.forEach((sniHostName, routing) -> {
                HotRodServer hotRod = routing.getHotRod().getValue();
                SecurityRealm securityRealm = routing.getSecurityRealm().getValue();

                SniNettyRouteSource source = new SniNettyRouteSource(sniHostName, securityRealm.getSSLContext());
                HotRodServerRouteDestination destination = new HotRodServerRouteDestination(hotRod.getQualifiedName(), hotRod);

                configurationBuilder.routing().add(new Route<>(source, destination));
            });

            restRouting.forEach((path, routing) -> {
                RestServer restResource = routing.getRest().getValue();
                String name = routing.getName();

                RestRouteSource source = new RestRouteSource(path);
                RestServerRouteDestination destination = new RestServerRouteDestination(name, restResource);

                configurationBuilder.routing().add(new Route<>(source, destination));
            });

            SecurityRealm singlePortSecurityRealm = singlePortRouting.getSecurityRealm().getOptionalValue();
            RestServer singlePortRestServer = singlePortRouting.getRestServer().getOptionalValue();
            HotRodServer singlePortHotRodServer = singlePortRouting.getHotrodServer().getOptionalValue();

            if (singlePortRestServer != null || singlePortHotRodServer != null) {
                SinglePortRouteSource singlePortRouteSource = new SinglePortRouteSource();

                if (singlePortRestServer != null) {
                    RestServerRouteDestination destination = new RestServerRouteDestination(singlePortRestServer.getQualifiedName(), singlePortRestServer);
                    configurationBuilder.routing().add(new Route<>(singlePortRouteSource, destination));
                }
                if (singlePortHotRodServer != null) {
                    HotRodServerRouteDestination destination = new HotRodServerRouteDestination(singlePortHotRodServer.getQualifiedName(), singlePortHotRodServer);
                    configurationBuilder.routing().add(new Route<>(singlePortRouteSource, destination));
                }
                if (singlePortSecurityRealm != null) {
                    features = checkSinglePortEnabled(features);
                    configurationBuilder.singlePort().sslWithAlpn(singlePortSecurityRealm.getSSLContext());
                }
            }

            this.router = new Router(configurationBuilder.build());
            this.router.start();

            ROOT_LOGGER.routerStarted(getAddressAsString(hotrodAddress), getAddressAsString(restAddress), getAddressAsString(singlePortAddress));
        } catch (Exception e) {
            throw ROOT_LOGGER.failedStart(e, name);
        }
    }

    private String getAddressAsString(InetSocketAddress address) {
        if (address == null) {
            return "None";
        }
        return NetworkUtils.formatAddress(address);
    }

    @Override
    public synchronized void stop(final StopContext context) {
        if (router != null) {
            router.stop();
        }
    }

    @Override
    public synchronized Router getValue() throws IllegalStateException {
        if (router == null) {
            throw ROOT_LOGGER.serviceNotStarted();
        }
        return router;
    }

    InjectedValue<SocketBinding> getHotrodSocketBinding() {
        return hotrodSocketBinding;
    }

    InjectedValue<SocketBinding> getSinglePortSocketBinding() {
        return singlePortSocketBinding;
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

    SinglePortRouting getSinglePortRouting() {
        return singlePortRouting;
    }

    public void tcpNoDelay(boolean tcpNoDelay) {
        this.configurationBuilder.hotrod().tcpNoDelay(tcpNoDelay);
    }

    public void tcpKeepAlive(boolean tcpKeepAlive) {
        this.configurationBuilder.hotrod().tcpKeepAlive(tcpKeepAlive);
    }

    public void sendBufferSize(int sendBufferSize) {
        this.configurationBuilder.hotrod().sendBufferSize(sendBufferSize);
    }

    public void receiveBufferSize(int receiveBufferSize) {
        this.configurationBuilder.hotrod().receiveBufferSize(receiveBufferSize);
    }
}
