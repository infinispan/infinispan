/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011-2013 Red Hat Inc. and/or its affiliates and other contributors
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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.NettyRestServer;
import org.infinispan.rest.configuration.ExtendedHeaders;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.as.domain.management.SecurityRealm;
import org.jboss.as.network.SocketBinding;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.dmr.ModelNode;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;

/**
 * A service which starts the REST web application
 *
 * @author Tristan Tarrant <ttarrant@redhat.com>
 * @since 6.0
 */
public class RestService implements Service<Lifecycle>, EncryptableService {
   private static final String DEFAULT_CONTEXT_PATH = "";
   private final InjectedValue<PathManager> pathManagerInjector = new InjectedValue<PathManager>();
   private final InjectedValue<EmbeddedCacheManager> cacheManagerInjector = new InjectedValue<EmbeddedCacheManager>();
   private final InjectedValue<SecurityDomainContext> securityDomainContextInjector = new InjectedValue<SecurityDomainContext>();
   private final InjectedValue<SocketBinding> socketBinding = new InjectedValue<SocketBinding>();
   private final InjectedValue<SecurityRealm> encryptionSecurityRealm = new InjectedValue<SecurityRealm>();
   private final Map<String, InjectedValue<SecurityRealm>> sniDomains = new HashMap<>();

   private final ModelNode config;
   private final String serverName;
   private Lifecycle restServer;
   private boolean clientAuth;

   public RestService(String serverName, ModelNode config) {
      this.serverName = serverName;
      this.config = config.clone();
   }

   private String cleanContextPath(String s) {
      if (s.endsWith("/")) {
         return s.substring(0, s.length() - 1);
      } else {
         return s;
      }
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void start(StartContext startContext) throws StartException {
      String path = this.config.hasDefined(ModelKeys.CONTEXT_PATH) ? cleanContextPath(this.config.get(ModelKeys.CONTEXT_PATH).asString()) : DEFAULT_CONTEXT_PATH;

      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
      builder.name(serverName);
      if (config.hasDefined(ModelKeys.IGNORED_CACHES)) {
         Set<String> ignoredCaches = config.get(ModelKeys.IGNORED_CACHES).asList()
               .stream().map(ModelNode::asString).collect(Collectors.toSet());
         builder.ignoredCaches(ignoredCaches);
      }
      builder.extendedHeaders(config.hasDefined(ModelKeys.EXTENDED_HEADERS)
            ? ExtendedHeaders.valueOf(config.get(ModelKeys.EXTENDED_HEADERS).asString())
            : ExtendedHeaders.ON_DEMAND);

      EncryptableServiceHelper.fillSecurityConfiguration(this, builder.ssl());

      String protocolName = getProtocolName();

      ROOT_LOGGER.endpointStarting(protocolName);
      try {
         SocketBinding socketBinding = getSocketBinding().getValue();
         InetSocketAddress socketAddress = socketBinding.getSocketAddress();
         builder.host(socketAddress.getAddress().getHostAddress());
         builder.port(socketAddress.getPort());
         restServer = NettyRestServer.createServer(builder.build(), cacheManagerInjector.getValue());
      } catch (Exception e) {
         throw ROOT_LOGGER.restContextCreationFailed(e);
      }

      try {
         restServer.start();
         ROOT_LOGGER.httpEndpointStarted(protocolName, path, "rest");
      } catch (Exception e) {
         throw ROOT_LOGGER.restContextStartFailed(e);
      }
   }

   private String getProtocolName() {
      return EncryptableServiceHelper.isSecurityEnabled(this) ?
            (EncryptableServiceHelper.isSniEnabled(this) ? serverName + "+SNI" : serverName + "+SSL") : serverName;
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void stop(StopContext stopContext) {
      restServer.stop();
   }

   /** {@inheritDoc} */
   @Override
   public synchronized Lifecycle getValue() throws IllegalStateException {
      if (restServer == null) {
         throw new IllegalStateException();
      }
      return restServer;
   }

   public InjectedValue<PathManager> getPathManagerInjector() {
      return pathManagerInjector;
   }

   public InjectedValue<EmbeddedCacheManager> getCacheManager() {
      return cacheManagerInjector;
   }

   public InjectedValue<SecurityDomainContext> getSecurityDomainContextInjector() {
      return securityDomainContextInjector;
   }

   public InjectedValue<SocketBinding> getSocketBinding() {
      return socketBinding;
   }

   @Override
   public InjectedValue<SecurityRealm> getEncryptionSecurityRealm() {
      return encryptionSecurityRealm;
   }

   @Override
   public InjectedValue<SecurityRealm> getSniSecurityRealm(String sniHostName) {
      return sniDomains.computeIfAbsent(sniHostName, v -> new InjectedValue<SecurityRealm>());
   }

   @Override
   public Map<String, InjectedValue<SecurityRealm>> getSniConfiguration() {
      return sniDomains;
   }

   @Override
   public String getServerName() {
      return serverName;
   }

   @Override
   public void setClientAuth(boolean enabled) {
      clientAuth = enabled;
   }

   @Override
   public boolean getClientAuth() {
      return clientAuth;
   }

}