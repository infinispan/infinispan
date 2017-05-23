package org.infinispan.rest.authentication;

import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.RestResponseException;

/**
 * Authentication mechanism.
 *
 * @author Sebastian Łaskawiec
 */
public interface Authenticator {

   /**
    * Challenges specific {@link InfinispanRequest} for authentication.
    *
    * @param request Request to be challenged.
    * @throws RestResponseException Thrown on error.
    * @throws AuthenticationException Thrown if authentication fails.
    */
   void challenge(InfinispanRequest request) throws RestResponseException;
}
