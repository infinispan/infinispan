package org.infinispan.server.core;

import org.infinispan.server.core.configuration.MockServerConfiguration;
import org.infinispan.server.core.configuration.MockServerConfigurationBuilder;
import org.infinispan.server.core.transport.NettyTransport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.group.ChannelMatcher;

public class MockProtocolServer extends AbstractProtocolServer<MockServerConfiguration> {

   public static final String DEFAULT_CACHE_NAME = "dummyCache";
   public static final int DEFAULT_PORT = 1245;

   public MockProtocolServer(String protocolName, NettyTransport transport) {
      super(protocolName);
      configuration = new MockServerConfigurationBuilder()
            .defaultCacheName(DEFAULT_CACHE_NAME)
            .port(DEFAULT_PORT)
            .build();
      this.transport = transport;
   }

   public MockProtocolServer() {
      super("MOCK");
   }

   public MockProtocolServer(MockServerConfiguration configuration) {
      this();
      this.configuration = configuration;
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
   public ChannelMatcher getChannelMatcher() {
      return channel -> true;
   }

   @Override
   public void installDetector(Channel ch) {
   }

   @Override
   public ChannelInitializer<Channel> getInitializer() {
      return null;
   }

   @Override
   protected String protocolType() {
      return "mock";
   }

   @Override
   protected String details() {
      return "undetailed";
   }
}
