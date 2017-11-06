package org.infinispan.rest;

import java.util.Arrays;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.authentication.impl.VoidAuthenticator;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.operations.CacheOperations;
import org.infinispan.server.core.AbstractProtocolServer;
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

   private Authenticator authenticator = new VoidAuthenticator();
   private CacheOperations cacheOperations;

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
      return new NettyInitializers(Arrays.asList(getRestChannelInitializer()));
   }

   /**
    * Returns Netty Channel Initializer for REST.
    *
    * @return Netty Channel Initializer for REST.
    */
   public RestChannelInitializer getRestChannelInitializer() {
      return new RestChannelInitializer(this, transport);
   }

   /**
    * Gets Authentication mechanism.
    *
    * @return {@link Authenticator} instance.
    */
   Authenticator getAuthenticator() {
      return authenticator;
   }

   CacheOperations getCacheOperations() {
      return cacheOperations;
   }

   /**
    * Sets Authentication mechanism.
    *
    * @param authenticator {@link Authenticator} instance.
    */
   public void setAuthenticator(Authenticator authenticator) {
      this.authenticator = authenticator;
   }

   @Override
   protected void startInternal(RestServerConfiguration configuration, EmbeddedCacheManager cacheManager) {
      super.startInternal(configuration, cacheManager);
      this.cacheOperations = new CacheOperations(configuration, new RestCacheManager<>(cacheManager, this::isCacheIgnored));
   }
}
