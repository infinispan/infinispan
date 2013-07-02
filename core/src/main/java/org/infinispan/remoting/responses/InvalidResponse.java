package org.infinispan.remoting.responses;

/**
 * An invalid response
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class InvalidResponse implements Response {
   @Override
   public boolean isValid() {
      return false;
   }

   @Override
   public boolean isSuccessful() {
      return false;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName();
   }
}
