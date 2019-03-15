package org.infinispan.rest;

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
import org.infinispan.rest.resources.CacheResource;
import org.infinispan.rest.resources.CacheResourceV2;
import org.infinispan.rest.resources.ClusterResource;
import org.infinispan.rest.resources.ConfigResource;
import org.infinispan.rest.resources.CounterResource;
import org.infinispan.rest.resources.SplashResource;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.transport.NettyInitializers;

import com.fasterxml.jackson.databind.ObjectMapper;

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
   private RestDispatcher restDispatcher;
   private RestCacheManager<Object> restCacheManager;
   private final ObjectMapper mapper = new ObjectMapper();

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
      super.stop();
      restCacheManager.stop();
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

      InvocationHelper invocationHelper = new InvocationHelper(restCacheManager,
            (EmbeddedCounterManager) EmbeddedCounterManagerFactory.asCounterManager(cacheManager), configuration, getExecutor());

      String rootContext = configuration.startTransport() ? configuration.contextPath() : "*";
      ResourceManager resourceManager = new ResourceManagerImpl(rootContext);

      resourceManager.registerResource(new CacheResource(invocationHelper));
      resourceManager.registerResource(new CacheResourceV2(invocationHelper));
      resourceManager.registerResource(new SplashResource());
      resourceManager.registerResource(new ConfigResource(cacheManager));
      resourceManager.registerResource(new CounterResource(invocationHelper));
      resourceManager.registerResource(new ClusterResource(cacheManager.getHealth(), mapper));

      this.restDispatcher = new RestDispatcherImpl(resourceManager);
   }
}
