package org.infinispan.server.core.transport

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since
 */

abstract class ExceptionEvent {
   def getCause: Throwable
}