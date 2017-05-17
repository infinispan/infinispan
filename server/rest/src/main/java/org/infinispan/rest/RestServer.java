package org.infinispan.rest;

import java.util.Arrays;

import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.authentication.impl.VoidAuthenticator;
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
   public Authenticator getAuthenticator() {
      return authenticator;
   }

   /**
    * Sets Authentication mechanism.
    *
    * @param authenticator {@link Authenticator} instance.
    */
   public void setAuthenticator(Authenticator authenticator) {
      this.authenticator = authenticator;
   }
}
