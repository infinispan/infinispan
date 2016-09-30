package org.infinispan.server.core.transport;

import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.core.utils.SslUtils;

import io.netty.channel.Channel;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SniHandler;
import io.netty.util.DomainMappingBuilder;

/**
  * Pipeline factory for Netty based channels. For each pipeline created, a new decoder is created which means that
  * each incoming connection deals with a unique decoder instance. Since the encoder does not maintain any state,
  * a single encoder instance is shared by all incoming connections, if and only if, the protocol mandates an encoder.
  *
  * @author Galder Zamarreño
  * @author Sebastian Łaskawiec
  * @since 4.1
  */
public class NettyChannelInitializer<A extends ProtocolServerConfiguration> implements NettyInitializer {
   protected final ProtocolServer<A> server;
   protected final NettyTransport transport;
   protected final ChannelOutboundHandler encoder;

   public NettyChannelInitializer(ProtocolServer<A> server, NettyTransport transport, ChannelOutboundHandler encoder) {
      this.server = server;
      this.transport = transport;
      this.encoder = encoder;
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
   ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast("stats", new StatsChannelHandler(transport));
      SslConfiguration ssl = server.getConfiguration().ssl();
      if (ssl.enabled()) {
         //add default domain mapping
         JdkSslContext defaultNettySslContext = SslUtils.createNettySslContext(ssl, ssl.sniDomainsConfiguration().get("*"));
         DomainMappingBuilder<JdkSslContext> domainMappingBuilder = new DomainMappingBuilder<>(defaultNettySslContext);

         //and the rest
         ssl.sniDomainsConfiguration().forEach((k, v) -> {
            if (!"*".equals(k)) {
               domainMappingBuilder.add(k, SslUtils.createNettySslContext(ssl, v));
            }
         });

         pipeline.addLast("sni", new SniHandler(domainMappingBuilder.build()));
      }
      pipeline.addLast("decoder", server.getDecoder());
      if (encoder != null)
         pipeline.addLast("encoder", encoder);
   }
}
