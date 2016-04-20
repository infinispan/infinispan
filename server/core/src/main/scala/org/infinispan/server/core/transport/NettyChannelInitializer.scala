package org.infinispan.server.core.transport

import javax.net.ssl.SSLContext

import io.netty.channel.{Channel, ChannelInitializer, ChannelOutboundHandler}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import io.netty.handler.ssl._
import io.netty.util.DomainMappingBuilder
import org.infinispan.commons.util.SslContextFactory
import org.infinispan.server.core.ProtocolServer
import org.infinispan.server.core.configuration.{SslConfiguration, SslEngineConfiguration}

import scala.collection.JavaConverters._

/**
  * Pipeline factory for Netty based channels. For each pipeline created, a new decoder is created which means that
  * each incoming connection deals with a unique decoder instance. Since the encoder does not maintain any state,
  * a single encoder instance is shared by all incoming connections, if and only if, the protocol mandates an encoder.
  *
  * @author Galder Zamarreño
  * @author Sebastian Łaskawiec
  * @since 4.1
  */
class NettyChannelInitializer(server: ProtocolServer, transport: => NettyTransport
                              , encoder: ChannelOutboundHandler) extends ChannelInitializer[Channel] {

  override def initChannel(ch: Channel): Unit = {
    val pipeline = ch.pipeline
    pipeline.addLast("stats", new StatsChannelHandler(transport))
    val ssl = server.getConfiguration.ssl
    if (ssl.enabled()) {
      //add default domain mapping
      val defaultNettySslContext = createNettySslContext(ssl, ssl.sniDomainsConfiguration().get("*"))
      val domainMappingBuilder = new DomainMappingBuilder(defaultNettySslContext)

      //and the rest
      ssl.sniDomainsConfiguration().asScala
        .filterKeys(key => !"*".equals(key))
        .foreach(e => domainMappingBuilder.add(e._1, createNettySslContext(ssl, e._2)))

      pipeline.addLast("sni", new SniHandler(domainMappingBuilder.build()))
    }
    pipeline.addLast("decoder", server.getDecoder)
    if (encoder != null)
      pipeline.addLast("encoder", encoder)
    pipeline.addLast(new LoggingHandler(LogLevel.DEBUG))
  }

  private def createNettySslContext(sslConfiguration: SslConfiguration, sslEngineConfiguration: SslEngineConfiguration): JdkSslContext = {
    val sslContext = if (sslEngineConfiguration.sslContext != null) {
      sslEngineConfiguration.sslContext
    } else {
      SslContextFactory.getContext(sslEngineConfiguration.keyStoreFileName, sslEngineConfiguration.keyStorePassword,
        sslEngineConfiguration.keyStoreCertificatePassword, sslEngineConfiguration.trustStoreFileName, sslEngineConfiguration.trustStorePassword)
    }
    return createSslContext(sslContext, requireClientAuth(sslConfiguration))
  }

  private def createSslContext(sslContext: SSLContext, clientAuth: ClientAuth): JdkSslContext = {
    //Unfortunately we need to grap a list of available ciphers from the engine.
    //If we won't, JdkSslContext will use common ciphers from DEFAULT and SUPPORTED, which gives us 5 out of ~50 available ciphers
    //Of course, we don't need to any specific engine configuration here... just a list of ciphers
    val ciphers = SslContextFactory.getEngine(sslContext, false, false).getSupportedCipherSuites
    val context = new JdkSslContext(sslContext, false, ciphers.toIterable.asJava, IdentityCipherSuiteFilter.INSTANCE, null, clientAuth)
    return context
  }

  private def requireClientAuth(sslConfig: SslConfiguration): ClientAuth = sslConfig.requireClientAuth() match {
    case true => ClientAuth.REQUIRE
    case _ => ClientAuth.NONE
  }
}
