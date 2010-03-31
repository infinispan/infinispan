package org.infinispan.server.core.transport

/**
 * // TODO: Document this
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class ExceptionEvent {
   def getCause: Throwable
}