package org.infinispan.rest.authentication.impl;

import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.authentication.Authenticator;

/**
 * Accept all authentication mechanism.
 *
 * @author Sebastian Łaskawiec
 */
public class VoidAuthenticator implements Authenticator {

   @Override
   public void challenge(InfinispanRequest request) throws RestResponseException {
   }
}
