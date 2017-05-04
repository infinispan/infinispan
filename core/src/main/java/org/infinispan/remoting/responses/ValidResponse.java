package org.infinispan.remoting.responses;

/**
 * A valid response
 *
 * @author manik
 * @since 4.0
 */
public abstract class ValidResponse implements Response {

   @Override
   public boolean isValid() {
      return true;
   }

   public abstract Object getResponseValue();

   @Override
   public String toString() {
      return getClass().getSimpleName();
   }
}
