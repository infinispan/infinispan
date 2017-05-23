package org.infinispan.rest.authentication;

import java.security.Principal;

/**
 * Pluggable security domain which could be used as a bridge between {@link Authenticator} and
 * Wildfly Security Realms.
 */
public interface SecurityDomain {

   /**
    * Returns {@link Principal} based on user/password combination.
    *
    * @param username User name.
    * @param password Password.
    * @return Principal if authentication was successful.
    * @throws SecurityException Thrown in case of error or authentication failure.
    */
   Principal authenticate(String username, String password) throws SecurityException;
}
