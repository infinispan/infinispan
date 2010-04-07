package org.infinispan.server.core

import org.infinispan.manager.CacheManager
import transport.{Decoder, Encoder}

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
trait ProtocolServer {
   def start(host: String, port: Int, cacheManager: CacheManager, masterThreads: Int, workerThreads: Int)
   def stop
   def getEncoder: Encoder
   def getDecoder: Decoder
}