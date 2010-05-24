package org.infinispan.server.core

import transport.{Decoder, Encoder}
import org.infinispan.manager.{EmbeddedCacheManager}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
trait ProtocolServer {
   def start(host: String, port: Int, cacheManager: EmbeddedCacheManager, masterThreads: Int, workerThreads: Int, idleTimeout: Int)
   def stop
   def getEncoder: Encoder
   def getDecoder: Decoder
}
