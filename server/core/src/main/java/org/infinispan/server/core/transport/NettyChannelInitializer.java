package org.infinispan.server.core.transport;

import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.core.utils.SslUtils;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.ssl.ApplicationProtocolConfig;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SniHandler;
import io.netty.util.DomainNameMappingBuilder;

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
   protected final ChannelInboundHandler decoder;

   public NettyChannelInitializer(ProtocolServer<A> server, NettyTransport transport, ChannelOutboundHandler encoder, ChannelInboundHandler decoder) {
      this.server = server;
      this.transport = transport;
      this.encoder = encoder;
      this.decoder = decoder;
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      ChannelPipeline pipeline = ch.pipeline();
      if(transport != null) {
         pipeline.addLast("stats", new StatsChannelHandler(transport));
      }
      SslConfiguration ssl = server.getConfiguration().ssl();
      if (ssl.enabled()) {
         ApplicationProtocolConfig alpnConfig = getAlpnConfiguration();

         //add default domain mapping
         JdkSslContext defaultNettySslContext = SslUtils.createNettySslContext(ssl, ssl.sniDomainsConfiguration().get(SslConfiguration.DEFAULT_SNI_DOMAIN), alpnConfig);
         DomainNameMappingBuilder<JdkSslContext> domainMappingBuilder = new DomainNameMappingBuilder<>(defaultNettySslContext);

         //and the rest
         ssl.sniDomainsConfiguration().forEach((k, v) -> {
            if (!SslConfiguration.DEFAULT_SNI_DOMAIN.equals(k)) {
               domainMappingBuilder.add(k, SslUtils.createNettySslContext(ssl, v, alpnConfig));
            }
         });

         pipeline.addLast("sni", new SniHandler(domainMappingBuilder.build()));
      }
      if(decoder != null) {
         //We can not use `decoder` here. Each invocation creates a new instance of decoder and it seems
         //it can not be shared between pipelines.
         //See https://issues.jboss.org/browse/ISPN-7765 for more details.
         pipeline.addLast("decoder", server.getDecoder());
      }
      if (encoder != null)
         pipeline.addLast("encoder", encoder);
   }

   protected ApplicationProtocolConfig getAlpnConfiguration() {
      return null;
   }
}
