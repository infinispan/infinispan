package org.infinispan.rest.embedded.netty4;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseFilter;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.RestCacheManager;
import org.infinispan.rest.Server;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.embedded.netty4.security.Authenticator;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.logging.RestAccessLoggingHandler;
import org.infinispan.server.core.AbstractCacheIgnoreAware;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.core.utils.SslUtils;
import org.jboss.resteasy.spi.ResteasyDeployment;

import io.netty.handler.ssl.ClientAuth;

/**
 * Temporary fork from RestEasy 3.1.0
 */
public final class NettyRestServer extends AbstractCacheIgnoreAware implements Lifecycle {
   private final static Log log = LogFactory.getLog(NettyRestServer.class, Log.class);

   private static final String DEFAULT_REST_PATH = "rest";

   private final EmbeddedCacheManager cacheManager;
   private final RestServerConfiguration configuration;
   private final Authenticator authenticator;
   private NettyJaxrsServer netty;
   private Server server;

   private NettyRestServer(EmbeddedCacheManager cacheManager, RestServerConfiguration configuration, Authenticator authenticator) {
      this.cacheManager = cacheManager;
      this.configuration = configuration;
      this.authenticator = authenticator;
   }

   public static NettyRestServer createServer(RestServerConfiguration configuration, EmbeddedCacheManager manager) {
      return new NettyRestServer(manager, configuration, null);
   }

   public static NettyRestServer createServer(RestServerConfiguration configuration, EmbeddedCacheManager manager, Authenticator authenticator) {
      return new NettyRestServer(manager, configuration, authenticator);
   }

   private static void addEncryption(RestServerConfiguration config, NettyJaxrsServer netty) {
      if (config.ssl() != null && config.ssl().enabled()) {
         SslConfiguration sslConfig = config.ssl();
         SniConfiguration nettySniConfiguration = new SniConfiguration(SslUtils.createJdkSslContext(sslConfig, sslConfig.sniDomainsConfiguration().get("*")));

         sslConfig.sniDomainsConfiguration().forEach((domainName, domainConfiguration) -> {
            nettySniConfiguration.addSniMapping(domainName, SslUtils.createJdkSslContext(sslConfig, domainConfiguration));
         });

         netty.setSSLContext(sslConfig.sslContext());
         netty.setSniConfiguration(nettySniConfiguration);
      }
   }

   private static void startCaches(EmbeddedCacheManager cm) {
      // Start defined caches to avoid issues with lazily started caches
      cm.getCacheNames().forEach(name -> SecurityActions.getCache(cm, name));

      // Finally, start default cache as well
      cm.getCache();
   }

   private static EmbeddedCacheManager createCacheManager(String cfgFile) {
      try {
         return new DefaultCacheManager(cfgFile);
      } catch (IOException e) {
         log.errorReadingConfigurationFile(e, cfgFile);
         return new DefaultCacheManager();
      }
   }

   @Override
   public void start() {
      configuration.ignoredCaches().forEach(this::ignoreCache);
      RestCacheManager restCacheManager = new RestCacheManager(cacheManager, this::isCacheIgnored);
      server = new Server(configuration, restCacheManager);

      if (configuration.startTransport()) {
         NettyJaxrsServer nettyServer = createNetty();
         addEncryption(configuration, nettyServer);
         nettyServer.start();
         ResteasyDeployment deployment = nettyServer.getDeployment();
         deployment.getRegistry().addSingletonResource(server, DEFAULT_REST_PATH);
         deployment.getProviderFactory().register(new RestAccessLoggingHandler(), ContainerRequestFilter.class,
               ContainerResponseFilter.class);
         log.startRestServer(configuration.host(), configuration.port());
         netty = nettyServer;
      }
   }

   private NettyJaxrsServer createNetty() {
      // Start caches first, if not started
      startCaches(cacheManager);

      NettyJaxrsServer netty = new NettyJaxrsServer();
      ResteasyDeployment deployment = new ResteasyDeployment();
      netty.setDeployment(deployment);
      netty.setHostname(configuration.host());
      netty.setPort(configuration.port());
      netty.setRootResourcePath("");
      if (authenticator != null) {
         netty.setAuthenticator(authenticator);
      }
      return netty;
   }

   @Override
   public void stop() {
      if (netty != null) {
         netty.stop();
      }
      netty = null;
      server = null;
   }

   public Server getServer() {
      return server;
   }

   public EmbeddedCacheManager getCacheManager() {
      return cacheManager;
   }
}
