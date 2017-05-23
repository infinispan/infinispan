package org.infinispan.rest.server.authentication;

import org.infinispan.rest.server.InfinispanRequest;
import org.infinispan.rest.server.RestResponseException;

public interface Authenticator {
   void challenge(InfinispanRequest request) throws RestResponseException;
}
