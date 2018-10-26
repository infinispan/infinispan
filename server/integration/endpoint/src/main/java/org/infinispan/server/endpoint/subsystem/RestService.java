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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.authentication.SecurityDomain;
import org.infinispan.rest.authentication.impl.BasicAuthenticator;
import org.infinispan.rest.authentication.impl.ClientCertAuthenticator;
import org.infinispan.rest.authentication.impl.VoidAuthenticator;
import org.infinispan.rest.configuration.ExtendedHeaders;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.server.endpoint.subsystem.security.BasicRestSecurityDomain;
import org.jboss.as.controller.services.path.PathManager;
import org.jboss.as.domain.management.SecurityRealm;
import org.jboss.as.network.SocketBinding;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;

import io.netty.handler.codec.http.cors.CorsConfig;


/**
 * A service which starts the REST web application
 *
 * @author Tristan Tarrant &lt;ttarrant@redhat.com&gt;
 * @since 6.0
 */
public class RestService implements Service<RestServer>, EncryptableService {

   private static final int CROSS_ORIGIN_CONSOLE_PORT = 3000;

   private final InjectedValue<PathManager> pathManagerInjector = new InjectedValue<>();
   private final InjectedValue<EmbeddedCacheManager> cacheManagerInjector = new InjectedValue<>();
   private final InjectedValue<SocketBinding> socketBinding = new InjectedValue<>();
   private final InjectedValue<SocketBinding> socketBindingManagementPlain = new InjectedValue<>();
   private final InjectedValue<SocketBinding> socketBindingManagementSecured = new InjectedValue<>();
   private final InjectedValue<SecurityRealm> encryptionSecurityRealm = new InjectedValue<>();
   private final InjectedValue<SecurityRealm> authenticationSecurityRealm = new InjectedValue<>();
   private final Map<String, InjectedValue<SecurityRealm>> sniDomains = new HashMap<>();

   private final RestAuthMethod authMethod;
   private final String serverName;
   private final String contextPath;
   private final ExtendedHeaders extendedHeaders;
   private final Set<String> ignoredCaches;
   private RestServer restServer;
   private boolean clientAuth;
   private final int maxContentLength;
   private int compressionLevel;
   private List<CorsConfig> corsConfigList;

   public RestService(String serverName, RestAuthMethod authMethod, String contextPath, ExtendedHeaders extendedHeaders, Set<String> ignoredCaches,
                      int maxContentLength, int compressionLevel, List<CorsConfig> corsConfigList) {
      this.serverName = serverName;
      this.authMethod = authMethod;
      this.contextPath = contextPath;
      this.extendedHeaders = extendedHeaders;
      this.ignoredCaches = ignoredCaches;
      this.maxContentLength = maxContentLength;
      this.compressionLevel = compressionLevel;
      this.corsConfigList = corsConfigList;
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void start(StartContext startContext) throws StartException {
      RestServerConfigurationBuilder builder = new RestServerConfigurationBuilder();
      builder.name(serverName).extendedHeaders(extendedHeaders).ignoredCaches(ignoredCaches).contextPath(contextPath)
            .maxContentLength(maxContentLength).compressionLevel(compressionLevel);

      EncryptableServiceHelper.fillSecurityConfiguration(this, builder.ssl());

      String protocolName = getProtocolName();

      ROOT_LOGGER.endpointStarting(serverName);
      try {
         SocketBinding socketBinding = getSocketBinding().getOptionalValue();
         if (socketBinding == null) {
            builder.startTransport(false);
            ROOT_LOGGER.startingServerWithoutTransport("REST");
         } else {
            InetSocketAddress socketAddress = socketBinding.getSocketAddress();
            builder.host(socketAddress.getAddress().getHostAddress());
            builder.port(socketAddress.getPort());
         }

         int mgmtHttpPort = getSocketBindingManagementPlain().getValue().getAbsolutePort();
         int mgmtHttpsPort = getSocketBindingManagementSecured().getValue().getAbsolutePort();

         builder.corsAllowForLocalhost("http", mgmtHttpPort);
         builder.corsAllowForLocalhost("https", mgmtHttpsPort);
         builder.corsAllowForLocalhost("http", CROSS_ORIGIN_CONSOLE_PORT);
         builder.corsAllowForLocalhost("https", CROSS_ORIGIN_CONSOLE_PORT);
         builder.addAll(corsConfigList);

         Authenticator authenticator;
         switch (authMethod) {
            case BASIC: {
               SecurityRealm authenticationRealm = authenticationSecurityRealm.getOptionalValue();
               SecurityDomain restSecurityDomain = new BasicRestSecurityDomain(authenticationRealm);
               authenticator = new BasicAuthenticator(restSecurityDomain, authenticationRealm.getName());
               break;
            }
            case CLIENT_CERT: {
               if (!EncryptableServiceHelper.isSecurityEnabled(this)) {
                  throw ROOT_LOGGER.cannotUseCertificateAuthenticationWithoutEncryption();
               }
               authenticator = new ClientCertAuthenticator();
               break;
            }
            case NONE: {
               authenticator = new VoidAuthenticator();
               break;
            }
            default:
               throw ROOT_LOGGER.restAuthMethodUnsupported(authMethod.toString());
         }

         restServer = new RestServer();
         restServer.setAuthenticator(authenticator);
      } catch (Exception e) {
         throw ROOT_LOGGER.restContextCreationFailed(e);
      }

      try {
         restServer.start(builder.build(), cacheManagerInjector.getValue());
         ROOT_LOGGER.httpEndpointStarted(protocolName, restServer.getHost() + ":" + restServer.getPort(), contextPath);
      } catch (Exception e) {
         throw ROOT_LOGGER.restContextStartFailed(e);
      }
   }

   private String getProtocolName() {
      return EncryptableServiceHelper.isSecurityEnabled(this) ?
            (EncryptableServiceHelper.isSniEnabled(this) ? serverName + " (TLS/SNI)" : serverName + " (TLS)") : serverName;
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void stop(StopContext stopContext) {
      restServer.stop();
   }

   /** {@inheritDoc} */
   @Override
   public synchronized RestServer getValue() throws IllegalStateException {
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

   public InjectedValue<SecurityRealm> getAuthenticationSecurityRealm() {
      return authenticationSecurityRealm;
   }

   public InjectedValue<SocketBinding> getSocketBinding() {
      return socketBinding;
   }

   public InjectedValue<SocketBinding> getSocketBindingManagementPlain() {
      return socketBindingManagementPlain;
   }

   public InjectedValue<SocketBinding> getSocketBindingManagementSecured() {
      return socketBindingManagementSecured;
   }

   @Override
   public InjectedValue<SecurityRealm> getEncryptionSecurityRealm() {
      return encryptionSecurityRealm;
   }

   @Override
   public InjectedValue<SecurityRealm> getSniSecurityRealm(String sniHostName) {
      return sniDomains.computeIfAbsent(sniHostName, v -> new InjectedValue<>());
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
