package org.infinispan.util;

/**
 * Thrown when a cyclic dependency exist
 * @author gustavonalle
 * @since 7.0
 */
public class CyclicDependencyException extends Exception {
   public CyclicDependencyException(String message) {
      super(message);
   }

   protected CyclicDependencyException(String message, Throwable cause) {
      super(message, cause);
   }
}
