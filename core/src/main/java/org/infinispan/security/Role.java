package org.infinispan.security;

import java.util.Collection;

/**
 * A role to permission mapping.
 *
 * @author Tristan Tarrant
 * @since 7.0
 */
public interface Role {
   /**
    * Returns the name of this role
    */
   String getName();

   /**
    * Returns the list of permissions associated with this role
    */
   Collection<AuthorizationPermission> getPermissions();

   /**
    * Returns a pre-computed access mask which contains the permissions specified by this role
    */
   int getMask();
}
