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
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.core.transport.Transport;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.jboss.as.clustering.infinispan.DefaultCacheContainer;
import org.jboss.as.domain.management.SecurityRealm;
import org.jboss.as.network.NetworkUtils;
import org.jboss.as.network.SocketBinding;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.msc.service.Service;
import org.jboss.msc.service.StartContext;
import org.jboss.msc.service.StartException;
import org.jboss.msc.service.StopContext;
import org.jboss.msc.value.InjectedValue;

/**
 * The service that configures and starts the endpoints supported by data grid.
 *
 * @author Tristan Tarrant
 */
class ProtocolServerService implements Service<ProtocolServer>, EncryptableService {
   // The cacheManager that will be injected by the container (specified by the cacheContainer
   // attribute)
   private final InjectedValue<EmbeddedCacheManager> cacheManager = new InjectedValue<EmbeddedCacheManager>();
   // The cacheManager configuration service
   private final InjectedValue<GlobalConfiguration> cacheManagerConfiguration = new InjectedValue<GlobalConfiguration>();
   // The socketBinding that will be injected by the container
   private final InjectedValue<SocketBinding> socketBinding = new InjectedValue<SocketBinding>();
   // The security realm for authentication that will be injected by the container
   private final InjectedValue<SecurityRealm> authenticationSecurityRealm = new InjectedValue<SecurityRealm>();
   // The security domain to use for creating the SASL server subject (e.g. for the GSSAPI mech)
   private final InjectedValue<SecurityDomainContext> saslSecurityDomain = new InjectedValue<SecurityDomainContext>();
   // The security realm for encryption that will be injected by the container
   private final InjectedValue<SecurityRealm> encryptionSecurityRealm = new InjectedValue<SecurityRealm>();
   // A map of SNI host names and corresponding security domains
   private final Map<String, InjectedValue<SecurityRealm>> sniDomains = new HashMap<>();
   // Te extension manager
   private final InjectedValue<ExtensionManagerService> extensionManager = new InjectedValue<>();
   // The configuration for this service
   private final ProtocolServerConfigurationBuilder<?, ?> configurationBuilder;
   // The class which determines the type of server
   private final Class<? extends ProtocolServer> serverClass;
   // The server which handles the protocol
   private ProtocolServer protocolServer;

   // The transport used by the protocol server
   private Transport transport;
   // The name of the server
   private final String serverName;
   // The login context used to obtain the server subject
   private LoginContext serverLoginContext = null;
   private String serverContextName;
   private String defaultCacheName;
   private boolean clientAuth;

   ProtocolServerService(String serverName, Class<? extends ProtocolServer> serverClass, ProtocolServerConfigurationBuilder<?, ?> configurationBuilder) {
      this(serverName, serverClass, configurationBuilder, null);
   }

   ProtocolServerService(String serverName, Class<? extends ProtocolServer> serverClass, ProtocolServerConfigurationBuilder<?, ?> configurationBuilder, String defaultCacheName) {
      this.configurationBuilder = configurationBuilder;
      this.serverClass = serverClass;
      String serverTypeName = serverClass.getSimpleName();
      this.serverName = serverName != null ? serverTypeName + " " + serverName : serverTypeName;
      this.defaultCacheName = defaultCacheName;
   }

   @Override
   public synchronized void start(final StartContext context) throws StartException {
      ROOT_LOGGER.endpointStarting(serverName);

      boolean done = false;
      try {
         if (defaultCacheName != null) {
            configurationBuilder.defaultCacheName(defaultCacheName);
         } else {
            configurationBuilder.defaultCacheName(((DefaultCacheContainer) cacheManager.getValue()).getDefaultCacheName());
         }

         InetSocketAddress address = socketBinding.getOptionalValue() != null ? socketBinding.getValue().getSocketAddress() : null;
         if(address != null) {
            configurationBuilder.host(address.getAddress().getHostAddress());
            configurationBuilder.port(address.getPort());
         } else {
            configurationBuilder.startTransport(false);
            ROOT_LOGGER.startingServerWithoutTransport(serverName);
         }

         // FIXME: replace with a more appropriate operation handler once we have one that integrates with the DMR server
         configurationBuilder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());

         SecurityRealm encryptionRealm = encryptionSecurityRealm.getOptionalValue();

         final String qual;
         if (encryptionRealm != null) {
            EncryptableServiceHelper.fillSecurityConfiguration(this, configurationBuilder.ssl());
            qual = isSniEnabled() ? " (TLS+SNI)" : " (TLS)";
         } else {
            qual = "";
         }
         if (configurationBuilder instanceof HotRodServerConfigurationBuilder) { // FIXME: extend to all protocol servers once they support authn
            HotRodServerConfigurationBuilder hotRodBuilder = (HotRodServerConfigurationBuilder) configurationBuilder;

            if (serverContextName != null) {
               hotRodBuilder.authentication().serverSubject(getServerSubject(serverContextName));
            }
            SecurityRealm authenticationRealm = authenticationSecurityRealm.getOptionalValue();
            if (authenticationRealm != null) {
               hotRodBuilder.authentication().serverAuthenticationProvider(new EndpointServerAuthenticationProvider(authenticationRealm));
            }
         }


         ROOT_LOGGER.endpointStarted(serverName + qual, address != null ? NetworkUtils.formatAddress(address) : "None");
         // Start the connector
         startProtocolServer(configurationBuilder.build());

         addToExtensionManagerIfHotRod();

         done = true;
      } catch (StartException e) {
         throw e;
      } catch (Exception e) {
         throw ROOT_LOGGER.failedStart(e, serverName);
      } finally {
         if (!done) {
            doStop();
         }
      }
   }

   private boolean isSniEnabled() {
      return !sniDomains.isEmpty();
   }

   private void addToExtensionManagerIfHotRod() {
      // Similar thing done above, eventually this class should be split per protocol server type
      if (protocolServer instanceof HotRodServer)
         extensionManager.getValue().addHotRodServer((HotRodServer) protocolServer);
   }

   private void removeFromExtensionManagerIfHotRod() {
      // Similar thing done above, eventually this class should be split per protocol server type
      if (protocolServer instanceof HotRodServer)
         extensionManager.getValue().removeHotRodServer((HotRodServer) protocolServer);
   }

   private void startProtocolServer(ProtocolServerConfiguration configuration) throws StartException {
      // Start the server and record it
      ProtocolServer server;
      try {
         server = serverClass.newInstance();
      } catch (Exception e) {
         throw ROOT_LOGGER.failedConnectorInstantiation(e, serverName);
      }
      ROOT_LOGGER.connectorStarting(serverName);
      configuration.ignoredCaches().forEach(server::ignoreCache);
      SecurityActions.startProtocolServer(server, configuration, getCacheManager().getValue());
      protocolServer = server;

      try {
         transport = (Transport) ReflectionUtil.getValue(protocolServer, "transport");
      } catch (Exception e) {
         throw ROOT_LOGGER.failedTransportInstantiation(e.getCause(), serverName);
      }
   }

   @Override
   public synchronized void stop(final StopContext context) {
      doStop();
   }

   private void doStop() {
      try {
         if (protocolServer != null) {
            ROOT_LOGGER.connectorStopping(serverName);

            removeFromExtensionManagerIfHotRod();
            try {
               protocolServer.stop();
            } catch (Exception e) {
               ROOT_LOGGER.connectorStopFailed(e, serverName);
            }
         }
         if (serverLoginContext != null) {
            try {
               serverLoginContext.logout();
            } catch (LoginException e) {
            }
         }
      } finally {
         ROOT_LOGGER.connectorStopped(serverName);
      }
   }

   @Override
   public synchronized ProtocolServer getValue() throws IllegalStateException {
      if (protocolServer == null) {
         throw new IllegalStateException();
      }
      return protocolServer;
   }

   public InjectedValue<GlobalConfiguration> getCacheManagerConfiguration() {
      return cacheManagerConfiguration;
   }

   public InjectedValue<EmbeddedCacheManager> getCacheManager() {
      return cacheManager;
   }

   public InjectedValue<SocketBinding> getSocketBinding() {
      return socketBinding;
   }

   public InjectedValue<SecurityRealm> getAuthenticationSecurityRealm() {
      return authenticationSecurityRealm;
   }

   public InjectedValue<SecurityDomainContext> getSaslSecurityDomain() {
      return saslSecurityDomain;
   }

   @Override
   public InjectedValue<SecurityRealm> getEncryptionSecurityRealm() {
      return encryptionSecurityRealm;
   }

   public InjectedValue<ExtensionManagerService> getExtensionManager() {
      return extensionManager;
   }

    public Transport getTransport() {
      return transport;
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

   Subject getServerSubject(String serverSecurityDomain) throws LoginException
   {
      LoginContext lc = new LoginContext(serverSecurityDomain);
      lc.login();
      // Cache so we can log out.
      serverLoginContext  = lc;

      Subject serverSubject = serverLoginContext.getSubject();

      return serverSubject;
   }

   void setServerContextName(String serverContextName) {
      this.serverContextName = serverContextName;
   }

}
