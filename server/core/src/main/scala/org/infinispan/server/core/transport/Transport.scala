package org.infinispan.server.core.transport

import java.net.SocketAddress

/**
 * A transport.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class Transport {

   def start
   
   def stop
   
}