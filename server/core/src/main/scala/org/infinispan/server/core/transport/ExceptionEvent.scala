package org.infinispan.server.core.transport

/**
 * An exception event.
 * 
 * @author Galder Zamarreño
 * @since 4.1
 */
abstract class ExceptionEvent {
   def getCause: Throwable
}