package org.infinispan.util.logging;

/**
 * This exception is used to add stack trace information to exceptions as they move from one thread to another.
 *
 * @author Dan Berindei
 * @since 9.3
 */
public class TraceException extends Exception {
   public TraceException() {
   }
}
