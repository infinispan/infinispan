package org.infinispan.server.core

import transport.{Decoder, Encoder}
import org.infinispan.manager.{EmbeddedCacheManager}
import java.util.Properties

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
trait ProtocolServer {

   /**
    * Using Properties here instead of a Map in order to make it easier for java code to call in.
    */
   def start(properties: Properties, cacheManager: EmbeddedCacheManager)

   def stop

   def getEncoder: Encoder

   def getDecoder: Decoder
}
