package org.infinispan.server.core.security;

import java.security.Principal;
import java.util.Collection;

import javax.security.auth.Subject;

public interface SubjectUserInfo {

    /**
     * Get the name for this user.
     */
    String getUserName();

    /**
     * Get the principals for this user.

     */
    Collection<Principal> getPrincipals();

    /**
     * Returns the subject
     */
    Subject getSubject();

}