package org.infinispan.rest.server;

import java.util.Arrays;

import org.infinispan.rest.configuration.RestServerConfiguration;
import org.infinispan.rest.server.authentication.Authenticator;
import org.infinispan.rest.server.authentication.VoidAuthenticator;
import org.infinispan.server.core.AbstractProtocolServer;
import org.infinispan.server.core.transport.NettyInitializers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;

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
      return new NettyInitializers(Arrays.asList(new RestChannelInitializer(this, transport)));
   }

   public Authenticator getAuthenticator() {
      return authenticator;
   }

   public void setAuthenticator(Authenticator authenticator) {
      this.authenticator = authenticator;
   }
}
