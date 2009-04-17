package org.infinispan.remoting.responses;

/**
 * A valid response
 *
 * @author manik
 * @since 4.0
 */
public abstract class ValidResponse implements Response {

   public boolean isValid() {
      return true;
   }
}
