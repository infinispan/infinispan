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

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.NettyRestServer;
import org.infinispan.rest.configuration.ExtendedHeaders;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.jboss.as.controller.services.path.PathManager;
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
public class RestService implements Service<Lifecycle> {
   private static final String DEFAULT_CONTEXT_PATH = "";
   private final InjectedValue<PathManager> pathManagerInjector = new InjectedValue<PathManager>();
   private final InjectedValue<EmbeddedCacheManager> cacheManagerInjector = new InjectedValue<EmbeddedCacheManager>();
   private final InjectedValue<SecurityDomainContext> securityDomainContextInjector = new InjectedValue<SecurityDomainContext>();
   private final InjectedValue<SocketBinding> socketBinding = new InjectedValue<SocketBinding>();

   private final ModelNode config;
   private final String path;
   private final String securityDomain;
   private final String authMethod;
   private final SecurityMode securityMode;
   private PathManager.Callback.Handle callbackHandle;
   private final RestServerConfigurationBuilder configurationBuilder;
   private Lifecycle restServer;

   public RestService(ModelNode config) {
      this.config = config.clone();

      path = this.config.hasDefined(ModelKeys.CONTEXT_PATH) ? cleanContextPath(this.config.get(ModelKeys.CONTEXT_PATH).asString()) : DEFAULT_CONTEXT_PATH;
      securityDomain = config.hasDefined(ModelKeys.SECURITY_DOMAIN) ? config.get(ModelKeys.SECURITY_DOMAIN).asString() : null;
      authMethod = config.hasDefined(ModelKeys.AUTH_METHOD) ? config.get(ModelKeys.AUTH_METHOD).asString() : "BASIC";
      securityMode = config.hasDefined(ModelKeys.SECURITY_MODE) ? SecurityMode.valueOf(config.get(ModelKeys.SECURITY_MODE).asString()) : SecurityMode.READ_WRITE;

      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
      builder.extendedHeaders(config.hasDefined(ModelKeys.EXTENDED_HEADERS)
            ? ExtendedHeaders.valueOf(config.get(ModelKeys.EXTENDED_HEADERS).asString())
            : ExtendedHeaders.ON_DEMAND);
      configurationBuilder = builder;
   }

   private static String cleanContextPath(String s) {
      if (s.endsWith("/")) {
         return s.substring(0, s.length() - 1);
      } else {
         return s;
      }
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void start(StartContext startContext) throws StartException {
      ROOT_LOGGER.endpointStarting("REST");
      try {
         SocketBinding socketBinding = getSocketBinding().getValue();
         InetSocketAddress socketAddress = socketBinding.getSocketAddress();
         configurationBuilder.host(socketAddress.getAddress().getHostAddress());
         configurationBuilder.port(socketAddress.getPort());
         restServer = NettyRestServer.apply(configurationBuilder.build(), cacheManagerInjector.getValue());
      } catch (Exception e) {
         throw ROOT_LOGGER.restContextCreationFailed(e);
      }

      try {
         restServer.start();
         ROOT_LOGGER.httpEndpointStarted("REST", path, "rest");
      } catch (Exception e) {
         throw ROOT_LOGGER.restContextStartFailed(e);
      }
   }

   public String getSecurityDomain() {
      return securityDomain;
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void stop(StopContext stopContext) {
      restServer.stop();
   }

   String getCacheContainerName() {
      if (!config.hasDefined(ModelKeys.CACHE_CONTAINER)) {
         return null;
      }
      return config.get(ModelKeys.CACHE_CONTAINER).asString();
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

   InjectedValue<SocketBinding> getSocketBinding() {
      return socketBinding;
   }

}