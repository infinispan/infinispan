package org.infinispan.rest;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.authentication.impl.VoidAuthenticator;
import org.infinispan.rest.cachemanager.RestCacheManager;
import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.operations.CacheOperations;
import org.infinispan.rest.operations.SearchOperations;
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
   private SearchOperations searchOperations;

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

   SearchOperations getSearchOperations() {
      return searchOperations;
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
      RestCacheManager<Object> restCacheManager = new RestCacheManager<>(cacheManager, this::isCacheIgnored);
      this.cacheOperations = new CacheOperations(configuration, restCacheManager);
      this.searchOperations = new SearchOperations(configuration, restCacheManager);
   }

   @Override
   public int getWorkerThreads() {
      // Unused for now, so just return the smallest possible valid value
      return 1;
   }
}
