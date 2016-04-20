package org.infinispan.server.core.transport

import io.netty.handler.logging.{LogLevel, LoggingHandler}
import io.netty.util.{DomainMappingBuilder, DomainNameMapping}
import org.infinispan.server.core.ProtocolServer
import org.infinispan.server.core.configuration.SslConfiguration
import javax.net.ssl.{SSLContext, SSLEngine}
import org.infinispan.commons.util.SslContextFactory
import io.netty.channel.{ChannelInitializer, Channel, ChannelOutboundHandler}
import io.netty.handler.ssl._

/**
 * Pipeline factory for Netty based channels. For each pipeline created, a new decoder is created which means that
 * each incoming connection deals with a unique decoder instance. Since the encoder does not maintain any state,
 * a single encoder instance is shared by all incoming connections, if and only if, the protocol mandates an encoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyChannelInitializer(server: ProtocolServer, encoder: ChannelOutboundHandler) extends ChannelInitializer[Channel] {

   override def initChannel(ch: Channel): Unit = {
      val pipeline = ch.pipeline
      val ssl = server.getConfiguration.ssl
      if (ssl.enabled()) {
         val jdkSslContext = createSSLContext(ssl)
         val nettySslContext = createSslContext(jdkSslContext, requireClientAuth(ssl))
         val jdkSslEngine = SslContextFactory.getEngine(jdkSslContext, false, ssl.requireClientAuth)
         val domainMapping = new DomainMappingBuilder(nettySslContext).build()

         pipeline.addLast("sni", new SniHandler(domainMapping))
      }
      pipeline.addLast("decoder", server.getDecoder)
      if (encoder != null)
         pipeline.addLast("encoder", encoder)
      pipeline.addLast(new LoggingHandler(LogLevel.DEBUG))
   }

   def createSSLContext(ssl: SslConfiguration): SSLContext = {
      val sslContext = if (ssl.sslContext != null) {
         ssl.sslContext
      }
      SslContextFactory.getContext(ssl.keyStoreFileName, ssl.keyStorePassword, ssl.keyStoreCertificatePassword, ssl.trustStoreFileName, ssl.trustStorePassword)
   }

   def createSslContext(sslContext: SSLContext, clientAuth: ClientAuth): JdkSslContext = {
      return new JdkSslContext(sslContext, false, clientAuth)
   }

   def requireClientAuth(sslConfig: SslConfiguration): ClientAuth = sslConfig.requireClientAuth() match {
      case true => ClientAuth.REQUIRE
      case _ => ClientAuth.NONE
   }
}
