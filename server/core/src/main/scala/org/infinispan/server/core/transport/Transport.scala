package org.infinispan.server.core.transport

import java.net.SocketAddress

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

abstract class Transport {

   def start
   
   def stop
   
}