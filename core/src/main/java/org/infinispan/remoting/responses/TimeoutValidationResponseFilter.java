package org.infinispan.remoting.responses;

import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.util.concurrent.TimeoutException;

/**
 * A response filter that allows to validate its state after the timeout was expired.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
public interface TimeoutValidationResponseFilter extends ResponseFilter {

   /**
    * Validates the filter state. If it has no responses or some member didn't reply, it must throw a {@link
    * TimeoutException}
    *
    * @throws TimeoutException when a member didn't give a response.
    */
   void validate() throws TimeoutException;

}
