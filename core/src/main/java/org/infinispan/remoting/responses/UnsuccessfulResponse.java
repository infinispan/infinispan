package org.infinispan.remoting.responses;

/**
 * An unsuccessful response
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnsuccessfulResponse extends ValidResponse {
   public static final UnsuccessfulResponse INSTANCE = new UnsuccessfulResponse();

   private UnsuccessfulResponse() {
   }

   public boolean isSuccessful() {
      return false;
   }

   @Override
   public boolean equals(Object o) {
      return o.getClass().equals(UnsuccessfulResponse.class);
   }

   @Override
   public int hashCode() {
      return 13;
   }
}
