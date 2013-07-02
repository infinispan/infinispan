package org.infinispan.server.core.transport

import org.jboss.netty.channel._
import org.jboss.netty.handler.ssl.SslHandler
import org.infinispan.server.core.ProtocolServer
import org.infinispan.server.core.configuration.SslConfiguration
import javax.net.ssl.SSLEngine
import org.infinispan.commons.util.SslContextFactory

/**
 * Pipeline factory for Netty based channels. For each pipeline created, a new decoder is created which means that
 * each incoming connection deals with a unique decoder instance. Since the encoder does not maintain any state,
 * a single encoder instance is shared by all incoming connections, if and only if, the protocol mandates an encoder.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
class NettyChannelPipelineFactory(server: ProtocolServer,
                                  encoder: ChannelDownstreamHandler)
      extends LifecycleChannelPipelineFactory {

   override def getPipeline: ChannelPipeline = {
      val pipeline = Channels.pipeline
      val ssl = server.getConfiguration.ssl
      if (ssl.enabled())
         pipeline.addLast("ssl", new SslHandler(createSslEngine(ssl)))
      pipeline.addLast("decoder", server.getDecoder)
      if (encoder != null)
         pipeline.addLast("encoder", encoder)

      return pipeline;
   }

   override def stop {
      // No-op
   }

   def createSslEngine(ssl: SslConfiguration): SSLEngine = {
      val sslContext = if (ssl.sslContext != null) {
         ssl.sslContext
      } else {
         SslContextFactory.getContext(ssl.keyStoreFileName, ssl.keyStorePassword, ssl.trustStoreFileName, ssl.trustStorePassword)
      }
      SslContextFactory.getEngine(sslContext, false, ssl.requireClientAuth)
   }
}
