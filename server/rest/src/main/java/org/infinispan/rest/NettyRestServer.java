package org.infinispan.rest;

import java.io.IOException;
import java.util.function.Consumer;

import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseFilter;

import org.infinispan.commons.api.Lifecycle;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.logging.RestAccessLoggingHandler;
import org.infinispan.server.core.AbstractCacheIgnoreAware;
import org.jboss.resteasy.plugins.server.netty.NettyJaxrsServer;
import org.jboss.resteasy.spi.ResteasyDeployment;

public final class NettyRestServer extends AbstractCacheIgnoreAware implements Lifecycle {
   final EmbeddedCacheManager cacheManager;
   private final RestServerConfiguration configuration;
   private final NettyJaxrsServer netty;
   private final Consumer<? super EmbeddedCacheManager> onStop;

   private final static Log log = LogFactory.getLog(NettyRestServer.class, Log.class);

   public static NettyRestServer createServer(RestServerConfiguration configuration) {
      return createServer(configuration, new DefaultCacheManager(), EmbeddedCacheManager::stop);
   }

   public static NettyRestServer createServer(RestServerConfiguration configuration, String configurationFile) {
      return createServer(configuration, createCacheManager(configurationFile), EmbeddedCacheManager::stop);
   }

   public static NettyRestServer createServer(RestServerConfiguration configuration, EmbeddedCacheManager manager) {
      return createServer(configuration, manager, cm -> {
      });
   }

   public static NettyRestServer createServer(RestServerConfiguration configuration, EmbeddedCacheManager manager,
                                              Consumer<? super EmbeddedCacheManager> consumer) {
      // Start caches first, if not started
      startCaches(manager);

      NettyJaxrsServer netty = new NettyJaxrsServer();
      ResteasyDeployment deployment = new ResteasyDeployment();
      netty.setDeployment(deployment);
      netty.setHostname(configuration.host());
      netty.setPort(configuration.port());
      netty.setRootResourcePath("");
      netty.setSecurityDomain(null);
      return new NettyRestServer(manager, configuration, netty, consumer);
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

   private NettyRestServer(EmbeddedCacheManager cacheManager, RestServerConfiguration configuration, NettyJaxrsServer netty,
                           Consumer<? super EmbeddedCacheManager> onStop) {
      this.cacheManager = cacheManager;
      this.configuration = configuration;
      this.netty = netty;
      this.onStop = onStop;
   }

   @Override
   public void start() {
      netty.start();
      ResteasyDeployment deployment = netty.getDeployment();
      configuration.getIgnoredCaches().forEach(this::ignoreCache);
      RestCacheManager restCacheManager = new RestCacheManager(cacheManager, this::isCacheIgnored);
      Server server = new Server(configuration, restCacheManager);
      deployment.getRegistry().addSingletonResource(server);
      deployment.getProviderFactory().register(new RestAccessLoggingHandler(), ContainerRequestFilter.class,
            ContainerResponseFilter.class);
      log.startRestServer(configuration.host(), configuration.port());
   }

   @Override
   public void stop() {
      netty.stop();
      onStop.accept(cacheManager);
   }
}
