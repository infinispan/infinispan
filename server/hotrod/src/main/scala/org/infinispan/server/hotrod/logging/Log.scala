package org.infinispan.server.hotrod.logging

import org.infinispan.util.logging.LogFactory

/**
 * A logging facade for Scala code.
 *
 * @author Galder Zamarreño
 * @since 5.0
 */
trait Log extends org.infinispan.server.core.logging.Log {

   private[hotrod] lazy val log: JavaLog = LogFactory.getLog(getClass, classOf[JavaLog])

   def logViewNullWhileDetectingCrashedMember = log.viewNullWhileDetectingCrashedMember

   def logUnableToUpdateView = log.unableToUpdateView

   def logErrorDetectingCrashedMember(t: Throwable) = log.errorDetectingCrashedMember(t)

}