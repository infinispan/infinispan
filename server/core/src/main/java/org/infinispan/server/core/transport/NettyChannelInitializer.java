package org.infinispan.server.core.transport;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

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
import io.netty.util.DomainWildcardMappingBuilder;

/**
 * Pipeline factory for Netty-based channels. For each pipeline created, a new decoder is created which means that each
 * incoming connection deals with a unique decoder instance. Since the encoder does not maintain any state, a single
 * encoder instance is shared by all incoming connections, if and only if, the protocol mandates an encoder.
 *
 * @author Galder Zamarreño
 * @author Sebastian Łaskawiec
 * @since 4.1
 */
public class NettyChannelInitializer<A extends ProtocolServerConfiguration> implements NettyInitializer {
   static final AtomicLong CHANNEL_ID = new AtomicLong();
   protected final ProtocolServer<A> server;
   protected final NettyTransport transport;
   protected final ChannelOutboundHandler encoder;
   protected final Supplier<ChannelInboundHandler> decoderSupplier;


   public NettyChannelInitializer(ProtocolServer<A> server, NettyTransport transport, ChannelOutboundHandler encoder, Supplier<ChannelInboundHandler> decoderSupplier) {
      this.server = server;
      this.transport = transport;
      this.encoder = encoder;
      this.decoderSupplier = decoderSupplier;
   }

   @Override
   public void initializeChannel(Channel ch) throws Exception {
      ConnectionMetadata info = ConnectionMetadata.getInstance(ch);
      info.id(CHANNEL_ID.getAndIncrement());
      info.created(Instant.now());
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast("iprules", new AccessControlFilter<>(server.getConfiguration()));
      if (transport != null) {
         pipeline.addLast("stats", new StatsChannelHandler(transport));
         SslConfiguration ssl = server.getConfiguration().ssl();
         if (ssl.enabled()) {
            ApplicationProtocolConfig alpnConfig = getAlpnConfiguration();

            //add default domain mapping
            JdkSslContext defaultNettySslContext = SslUtils.createNettySslContext(ssl, ssl.sniDomainsConfiguration().get(SslConfiguration.DEFAULT_SNI_DOMAIN), alpnConfig);
            DomainWildcardMappingBuilder<JdkSslContext> domainMappingBuilder = new DomainWildcardMappingBuilder<>(defaultNettySslContext);

            //and the rest
            ssl.sniDomainsConfiguration().forEach((k, v) -> {
               if (!SslConfiguration.DEFAULT_SNI_DOMAIN.equals(k)) {
                  domainMappingBuilder.add(k, SslUtils.createNettySslContext(ssl, v, alpnConfig));
               }
            });
            pipeline.addLast("sni", new SniHandler(domainMappingBuilder.build()));
         }
      }
      ChannelInboundHandler decoder = decoderSupplier != null ? decoderSupplier.get() : null;
      if (decoder != null) {
         pipeline.addLast("decoder", decoder);
      }
      if (encoder != null) {
         pipeline.addLast("encoder", encoder);
      }
   }

   protected ApplicationProtocolConfig getAlpnConfiguration() {
      return null;
   }
}
