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

   @volatile private var warnConditionalLogged = false
   @volatile private var warnForceReturnPreviousLogged = false

   def logViewNullWhileDetectingCrashedMember = log.viewNullWhileDetectingCrashedMember

   def logUnableToUpdateView = log.unableToUpdateView

   def logErrorDetectingCrashedMember(t: Throwable) = log.errorDetectingCrashedMember(t)

   def unexpectedEvent(e: Event[_, _]) = log.unexpectedEvent(e)

   def warnConditionalOperationNonTransactional(op: String) = {
      if (!warnConditionalLogged) {
         log.warnConditionalOperationNonTransactional(op)
         warnConditionalLogged = true
      }
   }

   def warnForceReturnPreviousNonTransactional(op: String) = {
      if (!warnForceReturnPreviousLogged) {
         log.warnForceReturnPreviousNonTransactional(op)
         warnForceReturnPreviousLogged = true
      }
   }

   def warnMarshallerAlreadySet(existingMarshaller: Marshaller, newMarshaller: Marshaller) =
      log.warnMarshallerAlreadySet(existingMarshaller, newMarshaller)

   def missingCacheEventFactory(factoryType: String, name: String) =
      log.missingCacheEventFactory(factoryType, name)

   def illegalFilterConverterEventFactory(name: String) =
      log.illegalFilterConverterEventFactory(name)

   def illegalIterationId(iterationId: String) = log.illegalIterationId(iterationId)

}