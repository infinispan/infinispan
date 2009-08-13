package org.infinispan.remoting.responses;

/**
 * An unsure response - used with Dist - essentially asks the caller to check the next response from the next node since
 * the sender is in a state of flux (probably in the middle of rebalancing)
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnsureResponse extends ValidResponse {
   public boolean isSuccessful() {
      return false;
   }
}
