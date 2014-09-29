package org.infinispan.server.hotrod.logging

import org.infinispan.commons.marshall.Marshaller
import org.infinispan.util.logging.LogFactory
import org.infinispan.notifications.cachelistener.event.Event

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

   def unexpectedEvent(e: Event[_, _]) = log.unexpectedEvent(e)

   def warnConditionalOperationNonTransactional(op: String) = log.warnConditionalOperationNonTransactional(op)

   def warnForceReturnPreviousNonTransactional(op: String) = log.warnForceReturnPreviousNonTransactional(op)

   def warnMarshallerAlreadySet(existingMarshaller: Marshaller, newMarshaller: Marshaller) =
      log.warnMarshallerAlreadySet(existingMarshaller, newMarshaller)
}