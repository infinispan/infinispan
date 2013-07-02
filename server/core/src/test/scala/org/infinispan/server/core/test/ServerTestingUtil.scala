package org.infinispan.server.core.test

import org.infinispan.server.core.logging.Log
import org.infinispan.server.core.AbstractProtocolServer

/**
 * Infinispan servers testing util
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
object ServerTestingUtil extends Log {

   def killServer(server: AbstractProtocolServer) {
      try {
         if (server != null) server.stop
      } catch {
         case t: Throwable => error("Error stopping server", t)
      }
   }

}
