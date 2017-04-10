package org.infinispan.rest.server.authentication;

import org.infinispan.rest.server.InfinispanRequest;
import org.infinispan.rest.server.RestResponseException;

public class VoidAuthenticator implements Authenticator {

   @Override
   public void challenge(InfinispanRequest request) throws RestResponseException {

   }
}
