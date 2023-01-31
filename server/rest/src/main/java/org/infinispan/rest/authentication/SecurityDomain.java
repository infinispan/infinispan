package org.infinispan.rest.authentication;

import javax.security.auth.Subject;

/**
 * Pluggable security domain which could be used as a bridge between {@link RestAuthenticator} and
 * WildFly Security Realms.
 */
public interface SecurityDomain {

   /**
    * Returns {@link Subject} based on user/password combination.
    *
    * @param username User name.
    * @param password Password.
    * @return Subject if authentication was successful.
    * @throws SecurityException Thrown in case of error or authentication failure.
    */
   Subject authenticate(String username, String password) throws SecurityException;
}
