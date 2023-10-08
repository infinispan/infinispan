package org.infinispan.remoting.responses;

/**
 * An invalid response
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface InvalidResponse extends Response {
   @Override
   default boolean isValid() {
      return false;
   }

   @Override
   default boolean isSuccessful() {
      return false;
   }
}
