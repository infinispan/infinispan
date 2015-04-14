package org.infinispan.rest.logging

import org.infinispan.util.logging.LogFactory

/**
 * A logging facade for Scala code.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
trait Log extends org.infinispan.server.core.logging.Log {

   private lazy val log: JavaLog = LogFactory.getLog(getClass, classOf[JavaLog])

   def logErrorReadingConfigurationFile(t: Throwable, path: String): Unit = {
      log.errorReadingConfigurationFile(t, path)
   }

   def logStartRestServer(host: String, port: Int): Unit = {
      log.startRestServer(host, port)
   }

}
