package org.infinispan.remoting.responses;

/**
 * A valid response
 *
 * @author manik
 * @since 4.0
 */
public interface ValidResponse<T> extends Response {

   T getResponseValue();

   @Override
   default boolean isValid() {
      return true;
   }

   @Override
   default boolean isSuccessful() {
      return true;
   }
}
