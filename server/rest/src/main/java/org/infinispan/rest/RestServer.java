package org.infinispan.rest;

import java.io.IOException;
import java.nio.file.Path;

import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.impl.manager.EmbeddedCounterManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.AuthenticationConfiguration;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.framework.ResourceManager;
import org.infinispan.rest.framework.RestDispatcher;
import org.infinispan.rest.framework.impl.ResourceManagerImpl;
import org.infinispan.rest.framework.impl.RestDispatcherImpl;
import org.infinispan.rest.resources.CacheManagerResource;
import org.infinispan.rest.resources.CacheResource;
import org.infinispan.rest.resources.CacheResourceV2;
import org.infinispan.rest.resources.CounterResource;
import org.infinispan.rest.resources.ServerResource;
import org.infinispan.rest.resources.SplashResource;
import org.infinispan.rest.resources.StaticFileResource;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.ServerManagement;
import org.infinispan.server.core.transport.NettyInitializers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;

/**
 * REST Protocol Server.
 *
 * @author Sebastian Łaskawiec
 */
public class RestServer extends AbstractProtocolServer<RestServerConfiguration> {
   private ServerManagement server;
   private RestDispatcher restDispatcher;
   private RestCacheManager<Object> restCacheManager;
   private InvocationHelper invocationHelper;

   public RestServer() {
      super("REST");
   }

   @Override
   public ChannelOutboundHandler getEncoder() {
      return null;
   }

   @Override
   public ChannelInboundHandler getDecoder() {
      return null;
   }

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      return new NettyInitializers(getRestChannelInitializer());
   }

   /**
    * Returns Netty Channel Initializer for REST.
    *
    * @return Netty Channel Initializer for REST.
    */
   public RestChannelInitializer getRestChannelInitializer() {
      return new RestChannelInitializer(this, transport);
   }

   RestDispatcher getRestDispatcher() {
      return restDispatcher;
   }

   @Override
   public void stop() {
      if (restCacheManager != null) {
         restCacheManager.stop();
      }
      if (invocationHelper != null) invocationHelper.stop();
      AuthenticationConfiguration auth = configuration.authentication();
      if (auth.enabled()) {
         try {
            auth.authenticator().close();
         } catch (IOException e) {
         }
      }
      super.stop();
   }

   public void setServer(ServerManagement server) {
      this.server = server;
   }

   @Override
   protected void startInternal(RestServerConfiguration configuration, EmbeddedCacheManager cacheManager) {
      this.configuration = configuration;
      AuthenticationConfiguration auth = configuration.authentication();
      if (auth.enabled()) {
         auth.authenticator().init(this);
      }
      super.startInternal(configuration, cacheManager);
      restCacheManager = new RestCacheManager<>(cacheManager, this::isCacheIgnored);

      invocationHelper = new InvocationHelper(restCacheManager,
            (EmbeddedCounterManager) EmbeddedCounterManagerFactory.asCounterManager(cacheManager),
            configuration, server, getExecutor());

      ResourceManager resourceManager = new ResourceManagerImpl(configuration.contextPath());

      resourceManager.registerResource(new CacheResource(invocationHelper));
      resourceManager.registerResource(new CacheResourceV2(invocationHelper));
      resourceManager.registerResource(new SplashResource());
      resourceManager.registerResource(new CounterResource(invocationHelper));
      resourceManager.registerResource(new CacheManagerResource(invocationHelper));
      Path staticResources = configuration.staticResources();
      if (staticResources != null) {
         resourceManager.registerResource(new StaticFileResource(staticResources, "static"));
      }
      if (server != null) {
         resourceManager.registerResource(new ServerResource(invocationHelper));
      }
      this.restDispatcher = new RestDispatcherImpl(resourceManager);
   }
}
