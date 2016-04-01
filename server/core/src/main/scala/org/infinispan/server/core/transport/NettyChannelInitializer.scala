package org.infinispan.server.core.transport

import javax.net.ssl.SSLEngine

import io.netty.channel.{Channel, ChannelInitializer, ChannelOutboundHandler}
import io.netty.handler.ssl.SslHandler
import org.infinispan.commons.util.SslContextFactory
import org.infinispan.server.core.ProtocolServer
import org.infinispan.server.core.configuration.SslConfiguration

/**
 * Pipeline factory for Netty based channels. For each pipeline created, a new decoder is created which means that
 * each incoming connection deals with a unique decoder instance. Since the encoder does not maintain any state,
 * a single encoder instance is shared by all incoming connections, if and only if, the protocol mandates an encoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyChannelInitializer(server: ProtocolServer, transport: => NettyTransport,
                                       encoder: ChannelOutboundHandler)
      extends ChannelInitializer[Channel] {

   override def initChannel(ch: Channel): Unit = {
      val pipeline = ch.pipeline
//      pipeline.addLast("netty-logging", new LoggingHandler(LogLevel.ERROR))
      pipeline.addLast("stats", new StatsChannelHandler(transport))
      val ssl = server.getConfiguration.ssl
      if (ssl.enabled())
         pipeline.addLast("ssl", new SslHandler(createSslEngine(ssl)))
      pipeline.addLast("decoder", server.getDecoder)
      if (encoder != null)
         pipeline.addLast("encoder", encoder)
   }

   def createSslEngine(ssl: SslConfiguration): SSLEngine = {
      val sslContext = if (ssl.sslContext != null) {
         ssl.sslContext
      } else {
         SslContextFactory.getContext(ssl.keyStoreFileName, ssl.keyStorePassword, ssl.keyStoreCertificatePassword(), ssl.trustStoreFileName, ssl.trustStorePassword)
      }
      SslContextFactory.getEngine(sslContext, false, ssl.requireClientAuth)
   }
}
