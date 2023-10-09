package org.infinispan.security;

import java.util.Collection;

import org.infinispan.security.impl.CacheRoleImpl;

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

   /**
    * Whether this role can be implicitly inherited.
    */
   boolean isInheritable();

   /**
    * A description for the role.
    */
   String getDescription();

   /**
    * If this role is part of the implicit authorization configuration
    */
   boolean isImplicit();

   static Role newRole(String name, String description, boolean isImplicit, boolean inheritable, AuthorizationPermission... authorizationPermissions) {
      return new CacheRoleImpl(name, description, isImplicit, inheritable, authorizationPermissions);
   }
}
