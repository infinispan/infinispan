package org.infinispan.server.core

import org.infinispan.manager.{EmbeddedCacheManager}
import java.util.Properties
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder
import org.jboss.netty.handler.codec.replay.ReplayingDecoder

/**
 * Represents a protocol compliant server.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
trait ProtocolServer {

   /**
    * Starts the server backed by the given cache manager and with the corresponding properties. If properties object
    * is either null or empty, default values depending on the server type are assumed. Note that properties mandate
    * String keys and values. Accepted property keys and default values are listed in {@link Main} class.
    */
   def start(properties: Properties, cacheManager: EmbeddedCacheManager)

   /**
    * Overloaded method that starts the server by using a properties file. This is particularly useful if trying to
    * start the cache through a beans.xml file or similar.
    */
   def start(propertiesFileName: String, cacheManager: EmbeddedCacheManager)

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
   def getDecoder: ReplayingDecoder[DecoderState]
}
