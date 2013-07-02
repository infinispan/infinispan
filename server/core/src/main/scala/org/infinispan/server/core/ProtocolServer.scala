package org.infinispan.server.core

import org.infinispan.manager.EmbeddedCacheManager
import java.util.Properties
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.channel.ChannelHandler
import org.infinispan.server.core.configuration.ProtocolServerConfiguration
import org.infinispan.server.core.transport.LifecycleChannelPipelineFactory

/**
 * Represents a protocol compliant server.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
trait ProtocolServer {
   type SuitableConfiguration <: ProtocolServerConfiguration

   /**
    * Starts the server backed by the given cache manager and with the corresponding configuration.
    */
   def start(configuration: SuitableConfiguration, cacheManager: EmbeddedCacheManager)

   /**
    *  Stops the server
    */
   def stop

   /**
    * Gets the encoder for this protocol server. The encoder is responsible for writing back common header responses
    * back to client. This method can return null if the server has no encoder. You can find an example of the server
    * that has no encoder in the Memcached server.
    */
   def getEncoder: OneToOneEncoder

   /**
    * Gets the decoder for this protocol server. The decoder is responsible for reading client requests.
    * This method cannot return null.
    */
   def getDecoder: ChannelHandler

   /**
    * Returns the configuration used to start this server
    */
   def getConfiguration: SuitableConfiguration

   /**
    * Returns a pipeline factory
    */
   def getPipeline: LifecycleChannelPipelineFactory
}
