package org.infinispan.server.core.transport;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.IdentityCipherSuiteFilter;
import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SniHandler;
import io.netty.util.DomainMappingBuilder;
import org.infinispan.commons.util.SslContextFactory;
import org.infinispan.server.core.ProtocolServer;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.core.configuration.SslConfiguration;
import org.infinispan.server.core.configuration.SslEngineConfiguration;

import javax.net.ssl.SSLContext;
import java.util.Arrays;

/**
  * Pipeline factory for Netty based channels. For each pipeline created, a new decoder is created which means that
  * each incoming connection deals with a unique decoder instance. Since the encoder does not maintain any state,
  * a single encoder instance is shared by all incoming connections, if and only if, the protocol mandates an encoder.
  *
  * @author Galder Zamarreño
  * @author Sebastian Łaskawiec
  * @since 4.1
  */
public class NettyChannelInitializer<A extends ProtocolServerConfiguration> extends ChannelInitializer<Channel> {
   protected final ProtocolServer<A> server;
   protected final NettyTransport transport;
   protected final ChannelOutboundHandler encoder;

   public NettyChannelInitializer(ProtocolServer<A> server, NettyTransport transport, ChannelOutboundHandler encoder) {
      this.server = server;
      this.transport = transport;
      this.encoder = encoder;
   }

   @Override
   protected void initChannel(Channel ch) throws Exception {
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast("stats", new StatsChannelHandler(transport));
      SslConfiguration ssl = server.getConfiguration().ssl();
      if (ssl.enabled()) {
         //add default domain mapping
         JdkSslContext defaultNettySslContext = createNettySslContext(ssl, ssl.sniDomainsConfiguration().get("*"));
         DomainMappingBuilder<JdkSslContext> domainMappingBuilder = new DomainMappingBuilder<>(defaultNettySslContext);

         //and the rest
         ssl.sniDomainsConfiguration().forEach((k, v) -> {
            if (!"*".equals(k)) {
               domainMappingBuilder.add(k, createNettySslContext(ssl, v));
            }
         });

         pipeline.addLast("sni", new SniHandler(domainMappingBuilder.build()));
      }
      pipeline.addLast("decoder", server.getDecoder());
      if (encoder != null)
         pipeline.addLast("encoder", encoder);
   }

   private JdkSslContext createNettySslContext(SslConfiguration sslConfiguration, SslEngineConfiguration sslEngineConfiguration) {
      SSLContext sslContext;
      if (sslEngineConfiguration.sslContext() != null) {
         sslContext = sslEngineConfiguration.sslContext();
      } else {
         sslContext = SslContextFactory.getContext(sslEngineConfiguration.keyStoreFileName(),
                 sslEngineConfiguration.keyStorePassword(), sslEngineConfiguration.keyStoreCertificatePassword(),
                 sslEngineConfiguration.trustStoreFileName(), sslEngineConfiguration.trustStorePassword());
      }
      return createSslContext(sslContext, requireClientAuth(sslConfiguration));
   }

  private JdkSslContext createSslContext(SSLContext sslContext, ClientAuth clientAuth) {
    //Unfortunately we need to grap a list of available ciphers from the engine.
    //If we won't, JdkSslContext will use common ciphers from DEFAULT and SUPPORTED, which gives us 5 out of ~50 available ciphers
    //Of course, we don't need to any specific engine configuration here... just a list of ciphers
    String[] ciphers = SslContextFactory.getEngine(sslContext, false, false).getSupportedCipherSuites();
    return new JdkSslContext(sslContext, false, Arrays.asList(ciphers), IdentityCipherSuiteFilter.INSTANCE, null, clientAuth);
  }

   private ClientAuth requireClientAuth(SslConfiguration sslConfig) {
      return sslConfig.requireClientAuth() ? ClientAuth.REQUIRE : ClientAuth.NONE;
   }
}
